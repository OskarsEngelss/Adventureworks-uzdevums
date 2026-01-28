import datetime
import pendulum
import math
import uuid

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from typing import Dict, List, Tuple, Optional, Any

# Connection and Config
POSTGRES_CONNECTION_ID = "postgres_source"
STARROCKS_CONNECTION_ID = "starrocks_mysql"
STAGING_DATABASE = "adventureworks_staging"

BATCH_SIZE = 5000  # Number of rows per StarRocks INSERT
FETCH_SIZE = 15000 # Number of rows to pull from Postgres at once
MAX_ROWS_PER_TASK = 500000  
MAX_TABLES_PER_RUN = 20  

ADVENTUREWORKS_SCHEMAS = [
    "sales", "person", "production", "purchasing", "humanresources"
]
FACT_KEYWORDS = [
    "order", "detail", "history", "transaction", "workorder",
    "inventory", "purchase", "salesorder"
]

def classify_table(table_name, columns):
    """Classifies table as fact or dimension based on name and content"""
    name = table_name.lower()
    column_names = [column.lower() for column in columns]

    if any(keyword in name for keyword in FACT_KEYWORDS):
        return "fact"

    measure_keywords = ["amount", "qty", "price", "total"]
    has_date = any("date" in column for column in column_names)
    has_measure = any(any(keyword in column for keyword in measure_keywords) for column in column_names)
    
    if has_date and has_measure:
        return "fact"

    return "dimension"

def map_postgres_to_starrocks(postgres_type):
    """Maps PostgreSQL data types to StarRocks equivalent"""
    postgres_type = postgres_type.lower()
    mapping = {
        'integer': 'INT',
        'bigint': 'BIGINT',
        'boolean': 'BOOLEAN',
        'timestamp without time zone': 'DATETIME',
        'timestamp with time zone': 'DATETIME',
        'date': 'DATE',
        'numeric': 'DECIMAL(38,9)',
        'double precision': 'DOUBLE',
        'character varying': 'VARCHAR(65533)',
        'varchar': 'VARCHAR(65533)',
        'text': 'VARCHAR(65533)',
        'char': 'CHAR',
        'uuid': 'VARCHAR(36)'
    }
    return mapping.get(postgres_type, 'VARCHAR(65533)')

def get_checkpoint(schema: str, table: str) -> str:
    """Get last processed checkpoint from Airflow Variables"""
    return Variable.get(f"checkpoint_{schema}_{table}", default_var="", deserialize_json=False)

def set_checkpoint(schema: str, table: str, checkpoint: str):
    """Set last processed checkpoint in Airflow Variables"""
    Variable.set(f"checkpoint_{schema}_{table}", checkpoint)

def create_insert_query(table_name: str, columns: List[str], batch: List[Tuple]) -> str:
    """Create multi-row INSERT statement with robust escaping"""
    placeholders = []
    for row in batch:
        formatted_values = []
        for value in row:
            if value is None:
                formatted_values.append("NULL")
            elif isinstance(value, str):
                escaped = value.replace("'", "''").replace("\\", "\\\\")
                formatted_values.append(f"'{escaped}'")
            elif isinstance(value, (datetime.datetime, datetime.date)):
                formatted_values.append(f"'{value.isoformat()}'")
            elif isinstance(value, bool):
                formatted_values.append("TRUE" if value else "FALSE")
            elif isinstance(value, (int, float)):
                formatted_values.append(str(value))
            else:
                formatted_values.append(f"'{str(value).replace("'", "''")}'")
        placeholders.append(f"({', '.join(formatted_values)})")
    
    columns_str = ', '.join([f"`{col}`" for col in columns])
    values_str = ', '.join(placeholders)
    return f"INSERT INTO {table_name} ({columns_str}) VALUES {values_str}"

@dag(
    dag_id="synchronize_postgresql_to_starrocks",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    max_active_tasks=3,
    tags=["staging", "starrocks", "adventureworks", "incremental"]
)
def synchronize_postgresql_to_starrocks():

    @task
    def discover_tables():
        """Discover tables and select which ones to process"""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        with hook.get_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema IN %s AND table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name;
                """, (tuple(ADVENTUREWORKS_SCHEMAS),))
                tables = cursor.fetchall()

                results = []
                for schema, table in tables:
                    cursor.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position;
                    """, (schema, table))
                    raw_columns = cursor.fetchall()

                    columns_with_types = []
                    for column_name, column_type in raw_columns:
                        if column_type.lower() in ['xml', 'bytea', 'user-defined'] or 'photo' in column_name.lower():
                            continue 
                        
                        columns_with_types.append({"name": column_name, "type": column_type})

                    just_names = [column["name"] for column in columns_with_types]
                    if not just_names: continue

                    checkpoint = get_checkpoint(schema, table)
                    results.append({
                        "schema": schema,
                        "table": table,
                        "columns": just_names,
                        "column_types": {col["name"]: col["type"] for col in columns_with_types},
                        "type": classify_table(table, just_names),
                        "checkpoint": checkpoint,
                        "is_complete": checkpoint == "COMPLETE"
                    })
        
        incomplete_tables = [t for t in results if not t["is_complete"]]
        return incomplete_tables[:MAX_TABLES_PER_RUN]

    @task
    def prepare_starrocks_table(table_config: dict):
        """Create or verify StarRocks table structure"""
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        schema, table_name = table_config['schema'], table_config['table']
        columns, column_types = table_config['columns'], table_config['column_types']
        full_table_name = f"{STAGING_DATABASE}.stg_{schema}_{table_name}"
        
        starrocks_hook.run(f"CREATE DATABASE IF NOT EXISTS {STAGING_DATABASE};")
        
        try:
            starrocks_hook.run(f"SELECT 1 FROM {full_table_name} LIMIT 1")
        except Exception:
            starrocks_column_definitions = []
            for column in columns:
                starrocks_type = map_postgres_to_starrocks(column_types.get(column, 'text'))
                starrocks_column_definitions.append(f"`{column}` {starrocks_type}")
            
            distribution_key = columns[0]
            starrocks_hook.run(f"""
                CREATE TABLE {full_table_name} (
                    {', '.join(starrocks_column_definitions)}
                ) 
                ENGINE=OLAP
                DUPLICATE KEY(`{distribution_key}`)
                DISTRIBUTED BY HASH(`{distribution_key}`) BUCKETS 8
                PROPERTIES ("replication_num" = "1");
            """)
        return table_config

    @task
    def process_table_batch(table_config: dict):
        """Processes rows with fast connection cycling and composite ordering"""
        schema, table_name = table_config['schema'], table_config['table']
        columns, column_types = table_config['columns'], table_config['column_types']
        full_table_name = f"{STAGING_DATABASE}.stg_{schema}_{table_name}"
        checkpoint = get_checkpoint(schema, table_name)
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        sr_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        # 1. Determine Identity/Ordering columns
        pk_col = None
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT kcu.column_name FROM information_schema.key_column_usage kcu
                    WHERE kcu.table_schema = %s AND kcu.table_name = %s LIMIT 1
                """, (schema, table_name))
                res = cur.fetchone()
                pk_col = res[0] if res else columns[0]

        inc_col = "ModifiedDate" if "ModifiedDate" in columns else pk_col
        inc_idx = columns.index(inc_col)
        inc_type = column_types[inc_col].lower()

        total_processed = 0
        # Initialize starting point
        current_checkpoint = checkpoint if checkpoint and checkpoint != "COMPLETE" else ("0" if "int" in inc_type else "1900-01-01")

        while total_processed < MAX_ROWS_PER_TASK:
            # 2. Open connection, fetch a large chunk, then close immediately
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Optimized casting: only cast to text if not a standard sortable type
                    cast = "::text" if "timestamp" not in inc_type and "int" not in inc_type and "date" not in inc_type else ""
                    query = f"""
                        SELECT {", ".join([f'"{c}"' for c in columns])} 
                        FROM "{schema}"."{table_name}" 
                        WHERE "{inc_col}"{cast} > %s 
                        ORDER BY "{inc_col}" ASC, "{pk_col}" ASC 
                        LIMIT %s
                    """
                    cur.execute(query, (current_checkpoint, FETCH_SIZE))
                    rows = cur.fetchall()

            if not rows:
                set_checkpoint(schema, table_name, "COMPLETE")
                return {"schema": schema, "table": table_name, "complete": True}

            # 3. Process into StarRocks in smaller transaction batches
            for i in range(0, len(rows), BATCH_SIZE):
                chunk = rows[i:i + BATCH_SIZE]
                sr_hook.run(create_insert_query(full_table_name, columns, chunk))
                total_processed += len(chunk)

            # Update checkpoint to the last row processed in this large chunk
            current_checkpoint = str(rows[-1][inc_idx])
            set_checkpoint(schema, table_name, current_checkpoint)
            
            if len(rows) < FETCH_SIZE:
                set_checkpoint(schema, table_name, "COMPLETE")
                return {"schema": schema, "table": table_name, "complete": True}

        return {"schema": schema, "table": table_name, "complete": False}

    @task
    def monitor_progress(table_results: List[dict]):
        done = [f"{r['schema']}.{r['table']}" for r in table_results if r.get('complete')]
        print(f"Summary - Completed: {len(done)}, Total Tables in run: {len(table_results)}")

    # DAG Flow
    table_list = discover_tables()
    prepared_tables = prepare_starrocks_table.expand(table_config=table_list)
    batch_results = process_table_batch.expand(table_config=prepared_tables)
    monitor_progress(batch_results)

synchronize_postgresql_to_starrocks()