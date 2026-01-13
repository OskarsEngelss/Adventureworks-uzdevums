import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

POSTGRES_CONNECTION_ID = "postgres_source"
STARROCKS_CONNECTION_ID = "starrocks_mysql"
STAGING_DATABASE = "starrocks_staging"

STARROCKS_FRONTEND_HOST = "starrocks-fe-host" 
STARROCKS_FRONTEND_PORT = "8030"
STARROCKS_USER = "root"
STARROCKS_PASSWORD = ""

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
        'double precision': 'DOUBLE'
    }
    return mapping.get(postgres_type, 'VARCHAR(65533)')

@dag(
    dag_id="postgres_to_starrocks_elt_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    max_active_tasks=3,
    tags=["staging", "starrocks", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=20),
    }
)
def starrocks_staging_dag():

    @task
    def discover_tables():
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
                    "starrocks_type": map_postgres_to_starrocks(column_type)
                })

            just_names = [column["name"] for column in columns_with_types]
            if not just_names:
                continue

            results.append({
                "schema": schema,
                "table": table,
                "columns": just_names,
                "type": classify_table(table, just_names)
            })

        return results

    @task
    def synchronize_postgresql_to_starrocks(table_config: dict):
        from airflow.hooks.base import BaseHook
        import requests
        import os

        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        schema = table_config['schema']
        table_name = table_config['table']
        columns = table_config['columns']
        temp_file = f"/tmp/{schema}_{table_name}.csv"
        
        starrocks_column_definitions = [f"`{column}` STRING" for column in columns]
        distribution_key = columns[0]
        full_table_name = f"{STAGING_DATABASE}.stg_{schema}_{table_name}"
        
        starrocks_hook.run(f"CREATE DATABASE IF NOT EXISTS {STAGING_DATABASE};")
        starrocks_hook.run(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {', '.join(starrocks_column_definitions)}
            ) 
            ENGINE=OLAP
            DUPLICATE KEY(`{distribution_key}`)
            DISTRIBUTED BY HASH(`{distribution_key}`) BUCKETS 8;
        """)
        starrocks_hook.run(f"TRUNCATE TABLE {full_table_name};")

        cast_columns = [f'"{column}"::text' for column in columns]
        copy_sql = f"COPY (SELECT {', '.join(cast_columns)} FROM {schema}.\"{table_name}\") TO STDOUT WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"')"
        
        print(f"Streaming {schema}.{table_name} to local CSV...")
        postgres_hook.copy_expert(copy_sql, temp_file)

        connection = BaseHook.get_connection(STARROCKS_CONNECTION_ID)
        starrocks_host = str(connection.host).strip()
        starrocks_user = str(connection.login or "root").strip()
        starrocks_password = str(connection.password or "").strip()
        
        url = f"http://{starrocks_host}:8030/api/{STAGING_DATABASE}/stg_{schema}_{table_name}/_stream_load"
        
        column_list_string = ",".join([f"`{column.strip()}`" for column in columns])
        
        headers = {
            "Expect": "100-continue",
            "column_separator": ",",
            "row_delimiter": "\\n",
            "enclosed_by": "\"",
            "max_filter_ratio": "1.0",
            "columns": column_list_string,
            "strict_mode": "false"
        }
        
        print(f"Executing Stream Load for {full_table_name}...")
        try:
            with open(temp_file, 'rb') as file:
                response = requests.put(
                    url, 
                    data=file, 
                    headers=headers, 
                    auth=(starrocks_user, starrocks_password),
                    allow_redirects=False
                )

                if response.status_code == 307:
                    redirect_url = response.headers['Location'].replace("127.0.0.1", starrocks_host).replace("localhost", starrocks_host)
                    print(f"Redirecting to Backend: {redirect_url}")
                    file.seek(0)
                    response = requests.put(
                        redirect_url, 
                        data=file, 
                        headers=headers, 
                        auth=(starrocks_user, starrocks_password)
                    )

            response_json = response.json()
            if response.status_code != 200 or response_json.get("Status") != "Success":
                if response_json.get("Status") == "Fail" and "filtered rows" in response_json.get("Message", ""):
                    print(f"Warning: {response_json.get('NumberFilteredRows')} rows were filtered.")
                else:
                    raise Exception(f"Stream Load Failed: {response_json}")
            
            print(f"Successfully loaded {response_json.get('NumberLoadedRows')} rows.")

        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)

        return f"Success: {schema}.{table_name}"

    table_list = discover_tables()
    synchronize_postgresql_to_starrocks.expand(table_config=table_list)

starrocks_staging_dag()