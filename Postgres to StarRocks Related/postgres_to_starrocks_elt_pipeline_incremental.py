import datetime
import pendulum
import math
import uuid  # Added missing import

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from typing import Dict, List, Tuple, Optional, Any

POSTGRES_CONNECTION_ID = "postgres_source"
STARROCKS_CONNECTION_ID = "starrocks_mysql"
STAGING_DATABASE = "adventureworks_staging"

BATCH_SIZE = 5000  # Number of rows per batch
MAX_ROWS_PER_TASK = 500000  # Stop task after processing this many rows
MAX_TABLES_PER_RUN = 20  # Limit number of tables processed per run for incremental loading

ADVENTUREWORKS_SCHEMAS = [
    "sales", "person", "production", "purchasing", "humanresources"
]
FACT_KEYWORDS = [
    "order", "detail", "history", "transaction", "workorder",
    "inventory", "purchase", "salesorder"
]
DIMENSION_KEYWORDS = [
    "name", "description", "type", "category", "address",
    "currency", "creditcard", "product", "customer", "vendor"
]

def classify_table(table_name, columns):
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
    postgres_type = postgres_type.lower()
    mapping = {
        'integer': 'INT',
        'bigint': 'BIGINT',
        'boolean': 'BOOLEAN',
        'timestamp without time zone': 'DATETIME',
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

def get_checkpoint_key(schema: str, table: str) -> str:
    """Generate unique checkpoint key for table"""
    return f"checkpoint_{schema}_{table}"

def get_checkpoint(schema: str, table: str) -> str:
    """Get last processed checkpoint for table"""
    try:
        checkpoint_key = get_checkpoint_key(schema, table)
        return Variable.get(checkpoint_key, default_var="", deserialize_json=False)
    except Exception as e:
        print(f"Error getting checkpoint: {e}")
        return ""

def set_checkpoint(schema: str, table: str, checkpoint: str):
    """Set checkpoint for table"""
    try:
        checkpoint_key = get_checkpoint_key(schema, table)
        Variable.set(checkpoint_key, checkpoint)
    except Exception as e:
        print(f"Error setting checkpoint: {e}")

def create_insert_query(table_name: str, columns: List[str], batch: List[Tuple]) -> str:
    """Create multi-row INSERT statement"""
    placeholders = []
    for row in batch:
        formatted_values = []
        for value in row:
            if value is None:
                formatted_values.append("NULL")
            elif isinstance(value, str):
                escaped = value.replace("'", "''").replace("\\", "\\\\")
                formatted_values.append(f"'{escaped}'")
            elif isinstance(value, datetime.datetime):
                formatted_values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif isinstance(value, datetime.date):
                formatted_values.append(f"'{value.strftime('%Y-%m-%d')}'")
            elif isinstance(value, bool):
                formatted_values.append("TRUE" if value else "FALSE")
            elif isinstance(value, (int, float)):
                formatted_values.append(str(value))
            elif hasattr(value, '__str__'):  # Handle UUID and other objects
                formatted_values.append(f"'{str(value).replace("'", "''")}'")
            else:
                formatted_values.append("NULL")
        placeholders.append(f"({', '.join(formatted_values)})")
    
    columns_str = ', '.join([f"`{col}`" for col in columns])
    values_str = ', '.join(placeholders)
    
    return f"INSERT INTO {table_name} ({columns_str}) VALUES {values_str}"

def is_numeric_data_type(data_type: str) -> bool:
    """Check if data type is numeric"""
    numeric_types = [
        'integer', 'bigint', 'smallint', 'decimal', 'numeric',
        'real', 'double precision', 'float', 'serial', 'bigserial'
    ]
    return data_type.lower() in numeric_types

def get_default_checkpoint_for_type(data_type: str) -> Any:
    """Get appropriate default checkpoint value based on data type"""
    data_type_lower = data_type.lower()
    
    if 'uuid' in data_type_lower:
        return "00000000-0000-0000-0000-000000000000"
    elif is_numeric_data_type(data_type_lower):
        return 0
    elif any(text_type in data_type_lower for text_type in ['char', 'varchar', 'text', 'string']):
        return ""
    else:
        # Default to string comparison for unknown types
        return ""

def get_checkpoint_for_comparison(checkpoint_str: str, data_type: str) -> Any:
    """Convert checkpoint string to appropriate type for comparison"""
    if not checkpoint_str or checkpoint_str in ["0", ""]:
        return get_default_checkpoint_for_type(data_type)
    
    data_type_lower = data_type.lower()
    
    if is_numeric_data_type(data_type_lower):
        try:
            return int(checkpoint_str)
        except ValueError:
            try:
                return float(checkpoint_str)
            except ValueError:
                return 0
    else:
        # For strings, UUIDs, and other non-numeric types
        return checkpoint_str

@dag(
    dag_id="postgres_to_starrocks_elt_pipeline_incremental",
    schedule=None, # schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    max_active_tasks=3,
    tags=["staging", "starrocks", "adventureworks", "incremental"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def starrocks_staging_dag():

    @task
    def discover_tables():
        """Discover tables and select which ones to process in this run"""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        connection = hook.get_conn()
        cursor = connection.cursor()

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
                type_lower = column_type.lower()
                name_lower = column_name.lower()
                
                if type_lower in ['xml', 'bytea', 'user-defined'] or 'photo' in name_lower:
                    print(f"Skipping toxic/large column: {schema}.{table}.{column_name} ({column_type})")
                    continue 
                
                columns_with_types.append({
                    "name": column_name,
                    "type": column_type,
                    "starrocks_type": map_postgres_to_starrocks(column_type)
                })

            just_names = [column["name"] for column in columns_with_types]
            if not just_names:
                continue

            # Get table row count for planning
            cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            row_count = cursor.fetchone()[0]
            
            # Check if this table is already complete
            checkpoint = get_checkpoint(schema, table)
            is_complete = checkpoint == "COMPLETE"
            
            # Get column types for type-aware processing
            column_types = {col["name"]: col["type"] for col in columns_with_types}
            
            results.append({
                "schema": schema,
                "table": table,
                "columns": just_names,
                "column_types": column_types,
                "type": classify_table(table, just_names),
                "row_count": row_count,
                "checkpoint": checkpoint,
                "is_complete": is_complete
            })

        cursor.close()
        connection.close()
        
        # Select tables to process in this run
        incomplete_tables = [t for t in results if not t["is_complete"]]
        tables_to_process = incomplete_tables[:MAX_TABLES_PER_RUN]
        
        print(f"Found {len(results)} tables total")
        print(f"{len(incomplete_tables)} tables incomplete")
        print(f"Processing {len(tables_to_process)} tables in this run")
        
        return tables_to_process

    @task
    def prepare_starrocks_table(table_config: dict):
        """Create or verify StarRocks table structure"""
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        schema = table_config['schema']
        table_name = table_config['table']
        columns = table_config['columns']
        column_types = table_config.get('column_types', {})
        full_table_name = f"{STAGING_DATABASE}.stg_{schema}_{table_name}"
        
        # Create database if not exists
        starrocks_hook.run(f"CREATE DATABASE IF NOT EXISTS {STAGING_DATABASE};")
        
        # Check if table exists
        try:
            starrocks_hook.run(f"SELECT 1 FROM {full_table_name} LIMIT 1")
            print(f"Table {full_table_name} already exists")
        except Exception:
            # Create table with better type mapping if available
            starrocks_column_definitions = []
            for column in columns:
                col_type = column_types.get(column, 'VARCHAR(65533)')
                starrocks_type = map_postgres_to_starrocks(col_type)
                starrocks_column_definitions.append(f"`{column}` {starrocks_type}")
            
            distribution_key = columns[0]
            
            create_table_sql = f"""
                CREATE TABLE {full_table_name} (
                    {', '.join(starrocks_column_definitions)}
                ) 
                ENGINE=OLAP
                DUPLICATE KEY(`{distribution_key}`)
                DISTRIBUTED BY HASH(`{distribution_key}`) BUCKETS 8
                PROPERTIES ("replication_num" = "1");
            """
            
            starrocks_hook.run(create_table_sql)
            print(f"Created table {full_table_name}")
        
        return table_config

    @task
    def process_table_batch(table_config: dict):
        """Process a batch of rows from PostgreSQL to StarRocks using multi-row INSERT"""
        schema = table_config['schema']
        table_name = table_config['table']
        columns = table_config['columns']
        column_types = table_config.get('column_types', {})
        full_table_name = f"{STAGING_DATABASE}.stg_{schema}_{table_name}"
        
        # Get checkpoint
        checkpoint_str = get_checkpoint(schema, table_name)
        
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        # Get total rows for progress tracking
        postgres_conn = postgres_hook.get_conn()
        cursor = postgres_conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
        total_rows = cursor.fetchone()[0]
        cursor.close()
        postgres_conn.close()
        
        # If already marked as complete, skip
        if checkpoint_str == "COMPLETE":
            print(f"Table {schema}.{table_name} already complete")
            return {
                "schema": schema,
                "table": table_name,
                "processed": 0,
                "checkpoint": "COMPLETE",
                "complete": True
            }
        
        # Get primary key and its data type for type-aware pagination
        postgres_conn = postgres_hook.get_conn()
        cursor = postgres_conn.cursor()
        
        # Get primary key column with its data type
        cursor.execute("""
            SELECT kcu.column_name, c.data_type
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.columns c 
                ON kcu.table_schema = c.table_schema 
                AND kcu.table_name = c.table_name 
                AND kcu.column_name = c.column_name
            WHERE kcu.table_schema = %s AND kcu.table_name = %s 
            ORDER BY kcu.ordinal_position LIMIT 1
        """, (schema, table_name))
        
        pk_result = cursor.fetchone()
        cursor.close()
        postgres_conn.close()
        
        columns_str = ', '.join([f'"{col}"' for col in columns])
        total_processed = 0
        
        if pk_result and pk_result[0] in columns:
            # Use primary key for efficient pagination
            pk_column = pk_result[0]
            pk_data_type = pk_result[1]
            pk_index = columns.index(pk_column)
            
            print(f"Using primary key {pk_column} ({pk_data_type}) for pagination")
            
            # Get checkpoint value with proper type handling
            checkpoint_for_compare = get_checkpoint_for_comparison(checkpoint_str, pk_data_type)
            
            print(f"Starting from checkpoint: '{checkpoint_for_compare}' (type: {type(checkpoint_for_compare).__name__})")
            
            while total_processed < MAX_ROWS_PER_TASK:
                # Open new connection for each batch
                postgres_conn = postgres_hook.get_conn()
                cursor = postgres_conn.cursor()
                
                # Use CAST to TEXT for all comparisons to avoid type issues
                if is_numeric_data_type(pk_data_type):
                    # Faster for integers (like BusinessEntityID)
                    query = f"""
                        SELECT {columns_str} 
                        FROM "{schema}"."{table_name}" 
                        WHERE "{pk_column}" > %s
                        ORDER BY "{pk_column}" ASC
                        LIMIT %s
                    """
                else:
                    # Use your existing text cast only for UUIDs/Strings
                    query = f"""
                        SELECT {columns_str} 
                        FROM "{schema}"."{table_name}" 
                        WHERE "{pk_column}"::text > %s::text
                        ORDER BY "{pk_column}"::text ASC
                        LIMIT %s
                    """
                
                
                # Fetch more rows than BATCH_SIZE to allow for filtering
                limit_size = BATCH_SIZE * 3
                cursor.execute(query, (checkpoint_for_compare, limit_size))
                rows = cursor.fetchall()
                cursor.close()
                postgres_conn.close()
                
                if not rows:
                    # No more rows to process
                    print(f"No more rows to process for {schema}.{table_name}")
                    set_checkpoint(schema, table_name, "COMPLETE")
                    return {
                        "schema": schema,
                        "table": table_name,
                        "processed": total_processed,
                        "checkpoint": "COMPLETE",
                        "complete": True
                    }
                
                # Process in smaller batches for INSERT statements
                for i in range(0, len(rows), BATCH_SIZE):
                    batch = rows[i:i+BATCH_SIZE]
                    
                    if not batch:
                        continue
                    
                    # Create multi-row INSERT statement
                    insert_sql = create_insert_query(full_table_name, columns, batch)
                    
                    try:
                        starrocks_hook.run(insert_sql)
                        print(f"Successfully inserted {len(batch)} rows into {full_table_name}")
                        total_processed += len(batch)
                    except Exception as e:
                        error_msg = str(e)
                        print(f"Error inserting batch of {len(batch)} rows: {error_msg[:200]}")
                        
                        # Check for common issues
                        if "max_allowed_packet" in error_msg.lower():
                            print("Max allowed packet exceeded, trying smaller batches...")
                            # Try much smaller batches
                            for j in range(0, len(batch), 50):
                                small_batch = batch[j:j+50]
                                if small_batch:
                                    try:
                                        small_insert = create_insert_query(full_table_name, columns, small_batch)
                                        starrocks_hook.run(small_insert)
                                        print(f"Inserted {len(small_batch)} rows in smaller batch")
                                        total_processed += len(small_batch)
                                    except Exception as small_error:
                                        print(f"Failed to insert small batch: {str(small_error)[:200]}")
                                        continue
                        else:
                            # Fallback to individual rows for other errors
                            print("Trying individual rows...")
                            successful_rows = 0
                            for row in batch:
                                try:
                                    single_insert = create_insert_query(full_table_name, columns, [row])
                                    starrocks_hook.run(single_insert)
                                    successful_rows += 1
                                except Exception as single_error:
                                    print(f"Failed to insert row: {str(single_error)[:100]}")
                                    continue
                            total_processed += successful_rows
                            print(f"Inserted {successful_rows}/{len(batch)} rows individually")
                
                # Update checkpoint (last PK value)
                last_row = rows[-1]
                last_pk_value = last_row[pk_index]
                
                # Store as string for checkpoint
                checkpoint_str = str(last_pk_value)
                checkpoint_for_compare = get_checkpoint_for_comparison(checkpoint_str, pk_data_type)
                
                # Update checkpoint in Variables
                set_checkpoint(schema, table_name, checkpoint_str)
                
                print(f"Processed {total_processed} rows from {schema}.{table_name}, checkpoint: {checkpoint_str}")
                
                if total_processed >= MAX_ROWS_PER_TASK:
                    print(f"Reached max rows per task ({MAX_ROWS_PER_TASK}). Stopping for now.")
                    return {
                        "schema": schema,
                        "table": table_name,
                        "processed": total_processed,
                        "checkpoint": checkpoint_str,
                        "complete": False
                    }
                
                # Check if we've processed all available rows
                if len(rows) < limit_size:
                    print(f"Processed all available rows for {schema}.{table_name}")
                    set_checkpoint(schema, table_name, "COMPLETE")
                    return {
                        "schema": schema,
                        "table": table_name,
                        "processed": total_processed,
                        "checkpoint": "COMPLETE",
                        "complete": True
                    }
        else:
            # Use ROW_NUMBER() for pagination (works for all tables but less efficient)
            print(f"No primary key found, using ROW_NUMBER() for pagination")
            
            # Parse checkpoint as integer for ROW_NUMBER approach
            try:
                checkpoint = int(checkpoint_str) if checkpoint_str else 0
            except (ValueError, TypeError):
                checkpoint = 0
            
            while total_processed < MAX_ROWS_PER_TASK:
                # Open new connection for each batch
                postgres_conn = postgres_hook.get_conn()
                cursor = postgres_conn.cursor()
                
                # Use ROW_NUMBER() window function for consistent pagination
                query = f"""
                    WITH numbered_rows AS (
                        SELECT {columns_str}, 
                               ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn
                        FROM "{schema}"."{table_name}"
                    )
                    SELECT {columns_str}
                    FROM numbered_rows
                    WHERE rn > %s AND rn <= %s
                    ORDER BY rn
                """
                
                start_row = checkpoint
                end_row = checkpoint + (BATCH_SIZE * 3)
                
                cursor.execute(query, (start_row, end_row))
                rows = cursor.fetchall()
                cursor.close()
                postgres_conn.close()
                
                if not rows:
                    # No more rows to process
                    print(f"No more rows to process for {schema}.{table_name}")
                    set_checkpoint(schema, table_name, "COMPLETE")
                    return {
                        "schema": schema,
                        "table": table_name,
                        "processed": total_processed,
                        "checkpoint": "COMPLETE",
                        "complete": True
                    }
                
                # Process in smaller batches
                for i in range(0, len(rows), BATCH_SIZE):
                    batch = rows[i:i+BATCH_SIZE]
                    
                    if not batch:
                        continue
                    
                    # Create multi-row INSERT statement
                    insert_sql = create_insert_query(full_table_name, columns, batch)
                    
                    try:
                        starrocks_hook.run(insert_sql)
                        print(f"Successfully inserted {len(batch)} rows into {full_table_name}")
                        total_processed += len(batch)
                    except Exception as e:
                        print(f"Error inserting batch of {len(batch)} rows: {str(e)[:200]}")
                        # Fallback to individual rows
                        for row in batch:
                            try:
                                single_insert = create_insert_query(full_table_name, columns, [row])
                                starrocks_hook.run(single_insert)
                                total_processed += 1
                            except Exception:
                                continue
                
                # Update checkpoint
                checkpoint = checkpoint + len(rows)
                last_checkpoint = str(checkpoint)
                set_checkpoint(schema, table_name, last_checkpoint)
                
                print(f"Processed {total_processed} rows from {schema}.{table_name}, checkpoint: {last_checkpoint}")
                
                if total_processed >= MAX_ROWS_PER_TASK:
                    print(f"Reached max rows per task ({MAX_ROWS_PER_TASK}). Stopping for now.")
                    return {
                        "schema": schema,
                        "table": table_name,
                        "processed": total_processed,
                        "checkpoint": last_checkpoint,
                        "complete": False
                    }
        
        # If we get here without hitting limits, we processed all rows
        set_checkpoint(schema, table_name, "COMPLETE")
        print(f"Completed processing for {schema}.{table_name}")
        
        return {
            "schema": schema,
            "table": table_name,
            "processed": total_processed,
            "checkpoint": "COMPLETE",
            "complete": True
        }

    @task
    def monitor_progress(table_results: List[dict]):
        """Monitor which tables need more processing"""
        pending_tables = []
        completed_tables = []
        
        for result in table_results:
            schema = result['schema']
            table = result['table']
            complete = result.get('complete', False)
            
            if complete:
                completed_tables.append(f"{schema}.{table}")
            else:
                pending_tables.append({
                    "schema": schema,
                    "table": table,
                    "checkpoint": result.get('checkpoint', 0),
                    "processed": result.get('processed', 0)
                })
        
        print(f"Completed tables ({len(completed_tables)}): {completed_tables}")
        print(f"Pending tables ({len(pending_tables)}):")
        for table in pending_tables:
            print(f"  - {table['schema']}.{table['table']} (processed: {table['processed']}, checkpoint: {table['checkpoint']})")
        
        return {
            "completed": len(completed_tables),
            "pending": len(pending_tables),
            "total_tasks": len(table_results)
        }

    # Main DAG flow
    table_list = discover_tables()
    prepared_tables = prepare_starrocks_table.expand(table_config=table_list)
    batch_results = process_table_batch.expand(table_config=prepared_tables)
    progress_report = monitor_progress(batch_results)

starrocks_staging_dag()