import datetime
import pendulum
from typing import Dict, List, Tuple, Optional, Any

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable

# Database configuration
STAGING_DATABASE = "adventureworks_staging"      # For extracted raw data
VALIDATION_DATABASE = "adventureworks_validation"  # For validation results & errors
VALIDATION_RESULTS_TABLE = "validation_results"
ERROR_RECORDS_TABLE = "error_records"

# Email configuration for alerts
ALERT_EMAILS = ["data-team@company.com"]
MAX_FAILURE_PERCENTAGE = 5.0  # Allow up to 5% of records to fail validation

@dag(
    dag_id="adventureworks_data_quality_validation",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    max_active_tasks=3,
    tags=["validation", "data-quality", "starrocks", "adventureworks"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
        "email_on_failure": False,
    }
)
def adventureworks_validation_dag():

    @task
    def initialize_validation_database():
        """Create validation database and tables"""
        starrocks_hook = MySqlHook(mysql_conn_id="starrocks_mysql")
        
        # Create validation database
        starrocks_hook.run(f"CREATE DATABASE IF NOT EXISTS {VALIDATION_DATABASE};")
        print(f"Created/verified database: {VALIDATION_DATABASE}")
        
        # Create validation results table
        validation_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {VALIDATION_DATABASE}.{VALIDATION_RESULTS_TABLE} (
                table_name VARCHAR(255),
                validation_type VARCHAR(100),
                check_name VARCHAR(255),
                validation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP(),
                validation_id BIGINT,
                total_records BIGINT,
                failed_records BIGINT,
                failure_percentage DECIMAL(5,2),
                validation_status VARCHAR(50),
                details TEXT,
                source_database VARCHAR(100) DEFAULT '{STAGING_DATABASE}'
            )
            ENGINE=OLAP
            DUPLICATE KEY(table_name, validation_type, check_name, validation_timestamp)
            DISTRIBUTED BY HASH(table_name) BUCKETS 10
            PROPERTIES ("replication_num" = "1")
        """
        
        # Create error records table
        error_records_sql = f"""
            CREATE TABLE IF NOT EXISTS {VALIDATION_DATABASE}.{ERROR_RECORDS_TABLE} (
                table_name VARCHAR(255),
                validation_type VARCHAR(100),
                check_name VARCHAR(255),
                error_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP(),
                error_id BIGINT,
                record_identifier VARCHAR(1000),
                column_name VARCHAR(255),
                error_value TEXT,
                error_message TEXT,
                source_query TEXT,
                source_database VARCHAR(100) DEFAULT '{STAGING_DATABASE}',
                source_table VARCHAR(255)
            )
            ENGINE=OLAP
            DUPLICATE KEY(table_name, validation_type, check_name, error_timestamp)
            DISTRIBUTED BY HASH(table_name) BUCKETS 10
            PROPERTIES ("replication_num" = "1")
        """
        
        # Create sequence table for ID generation
        sequence_sql = f"""
            CREATE TABLE IF NOT EXISTS {VALIDATION_DATABASE}.validation_sequence (
                sequence_name VARCHAR(50),
                next_value BIGINT
            )
            ENGINE=OLAP
            DUPLICATE KEY(sequence_name)
            DISTRIBUTED BY HASH(sequence_name) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        
        try:
            starrocks_hook.run(validation_table_sql)
            starrocks_hook.run(error_records_sql)
            starrocks_hook.run(sequence_sql)
            
            # Initialize sequence
            init_sql = f"""
                INSERT INTO {VALIDATION_DATABASE}.validation_sequence (sequence_name, next_value)
                VALUES 
                    ('validation_id', 1),
                    ('error_id', 1)
                ON DUPLICATE KEY UPDATE next_value = next_value
            """
            starrocks_hook.run(init_sql)
            
            print(f"Initialized validation tables in {VALIDATION_DATABASE}")
            
        except Exception as e:
            print(f"Note creating tables (may already exist): {str(e)[:200]}")
        
        return True

    @task
    def get_tables_to_validate():
        """Get list of staging tables to validate"""
        starrocks_hook = MySqlHook(mysql_conn_id="starrocks_mysql")
        
        # Get all staging tables from the staging database
        tables_query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{STAGING_DATABASE}' 
            AND table_name LIKE 'stg_%'
            ORDER BY table_name
        """
        
        tables = starrocks_hook.get_records(tables_query)
        
        # Get source metadata from PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id="postgres_source")
        
        tables_with_metadata = []
        for table_row in tables:
            table_name = table_row[0]
            
            # Extract schema and table from staging table name
            parts = table_name.replace('stg_', '').split('_', 1)
            if len(parts) == 2:
                schema, original_table = parts
                
                # Get source table metadata
                postgres_conn = postgres_hook.get_conn()
                cursor = postgres_conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        c.column_name,
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        tc.constraint_type,
                        kcu.ordinal_position
                    FROM information_schema.columns c
                    LEFT JOIN information_schema.key_column_usage kcu 
                        ON c.table_schema = kcu.table_schema 
                        AND c.table_name = kcu.table_name 
                        AND c.column_name = kcu.column_name
                    LEFT JOIN information_schema.table_constraints tc
                        ON kcu.constraint_name = tc.constraint_name
                        AND kcu.table_schema = tc.table_schema
                        AND kcu.table_name = tc.table_name
                    WHERE c.table_schema = %s 
                        AND c.table_name = %s
                    ORDER BY c.ordinal_position
                """, (schema, original_table))
                
                columns = cursor.fetchall()
                cursor.close()
                postgres_conn.close()
                
                if columns:
                    column_metadata = []
                    for col in columns:
                        column_metadata.append({
                            "name": col[0],
                            "data_type": col[1],
                            "is_nullable": col[2] == 'YES',
                            "default_value": col[3],
                            "constraint_type": col[4],
                            "is_primary_key": col[4] == 'PRIMARY KEY',
                            "ordinal_position": col[5]
                        })
                    
                    tables_with_metadata.append({
                        "staging_table": table_name,
                        "schema": schema,
                        "source_table": original_table,
                        "columns": column_metadata,
                        "source_database": STAGING_DATABASE
                    })
        
        print(f"Found {len(tables_with_metadata)} tables to validate in {STAGING_DATABASE}")
        return tables_with_metadata

    def get_next_sequence_value(starrocks_hook, sequence_name: str) -> int:
        """Get next sequence value for ID generation"""
        try:
            select_sql = f"""
                SELECT next_value 
                FROM {VALIDATION_DATABASE}.validation_sequence 
                WHERE sequence_name = '{sequence_name}'
            """
            result = starrocks_hook.get_first(select_sql)
            
            if result:
                current_value = result[0]
                update_sql = f"""
                    UPDATE {VALIDATION_DATABASE}.validation_sequence 
                    SET next_value = next_value + 1 
                    WHERE sequence_name = '{sequence_name}'
                """
                starrocks_hook.run(update_sql)
                return current_value
            else:
                insert_sql = f"""
                    INSERT INTO {VALIDATION_DATABASE}.validation_sequence 
                    (sequence_name, next_value) VALUES ('{sequence_name}', 2)
                """
                starrocks_hook.run(insert_sql)
                return 1
        except Exception as e:
            print(f"Error getting sequence value: {e}")
            return int(datetime.datetime.now().timestamp() * 1000)

    @task
    def validate_table_data(table_metadata: dict):
        """Validate data in staging table"""
        starrocks_hook = MySqlHook(mysql_conn_id="starrocks_mysql")
        
        staging_table = table_metadata["staging_table"]
        source_database = table_metadata["source_database"]
        columns = table_metadata["columns"]
        
        full_source_table = f"{source_database}.{staging_table}"
        validation_results = []
        
        print(f"Validating table: {full_source_table}")
        
        try:
            # Get row count
            count_query = f"SELECT COUNT(*) FROM {full_source_table}"
            total_records_result = starrocks_hook.get_first(count_query)
            total_records = total_records_result[0] if total_records_result else 0
            
            if total_records == 0:
                print(f"Table {full_source_table} is empty")
                return {
                    "table_name": staging_table,
                    "source_database": source_database,
                    "total_checks": 0,
                    "failed_checks": 0,
                    "overall_status": "SKIP",
                    "total_records": 0
                }
            
            # Run validation checks...
            # (Your validation logic here, similar to before but storing in VALIDATION_DATABASE)
            
            # Example: Store a validation result
            validation_id = get_next_sequence_value(starrocks_hook, "validation_id")
            
            # Insert into validation database
            insert_sql = f"""
                INSERT INTO {VALIDATION_DATABASE}.{VALIDATION_RESULTS_TABLE} 
                (table_name, validation_type, check_name, validation_id, total_records, 
                 failed_records, failure_percentage, validation_status, details, source_database)
                VALUES (
                    '{staging_table.replace("'", "''")}',
                    'SAMPLE_CHECK',
                    'ROW_COUNT_CHECK',
                    {validation_id},
                    {total_records},
                    0,
                    0.0,
                    'PASS',
                    'Table has {total_records} rows',
                    '{source_database}'
                )
            """
            starrocks_hook.run(insert_sql)
            
            return {
                "table_name": staging_table,
                "source_database": source_database,
                "total_checks": 1,
                "failed_checks": 0,
                "overall_status": "PASS",
                "total_records": total_records
            }
            
        except Exception as e:
            print(f"Error validating {full_source_table}: {e}")
            return {
                "table_name": staging_table,
                "source_database": source_database,
                "total_checks": 0,
                "failed_checks": 1,
                "overall_status": "ERROR",
                "total_records": 0
            }

    @task
    def generate_validation_report(validation_results: List[dict]):
        """Generate summary report"""
        starrocks_hook = MySqlHook(mysql_conn_id="starrocks_mysql")
        
        # Calculate statistics - FIXED variable names
        total_tables = len(validation_results)
        failed_tables = sum(1 for result in validation_results if result.get('overall_status') in ['FAIL', 'ERROR'])
        total_checks = sum(result.get('total_checks', 0) for result in validation_results)
        failed_checks = sum(result.get('failed_checks', 0) for result in validation_results)
        total_records = sum(result.get('total_records', 0) for result in validation_results)
        
        failure_percentage = (failed_checks / total_checks * 100) if total_checks > 0 else 0
        
        print("=" * 80)
        print("DATA VALIDATION REPORT")
        print("=" * 80)
        print(f"Tables validated: {total_tables}")
        print(f"Tables failed: {failed_tables}")
        print(f"Total checks: {total_checks}")
        print(f"Failed checks: {failed_checks}")
        print(f"Failure percentage: {failure_percentage:.2f}%")
        print(f"Total records processed: {total_records}")
        print("=" * 80)
        
        # List failed tables
        if failed_tables > 0:
            print("\nFAILED TABLES:")
            for result in validation_results:
                status = result.get('overall_status', 'UNKNOWN')
                if status in ['FAIL', 'ERROR']:
                    table_name = result.get('table_name', 'Unknown')
                    failed_checks = result.get('failed_checks', 0)
                    total_checks = result.get('total_checks', 0)
                    print(f"  - {table_name}: {failed_checks}/{total_checks} checks failed")
        
        # Store summary in validation database
        try:
            validation_id = get_next_sequence_value(starrocks_hook, "validation_id")
        except Exception:
            validation_id = int(datetime.datetime.now().timestamp() * 1000)
            
        summary_sql = f"""
            INSERT INTO {VALIDATION_DATABASE}.{VALIDATION_RESULTS_TABLE} 
            (table_name, validation_type, check_name, validation_id, total_records, 
            failed_records, failure_percentage, validation_status, details, source_database)
            VALUES (
                'ALL_TABLES',
                'SUMMARY',
                'VALIDATION_REPORT',
                {validation_id},
                {total_records},
                {failed_checks},
                {failure_percentage},
                '{"FAIL" if failure_percentage > MAX_FAILURE_PERCENTAGE else "PASS"}',
                'Validated {total_tables} tables from {STAGING_DATABASE}. {failed_checks}/{total_checks} checks failed.',
                '{STAGING_DATABASE}'
            )
        """
        
        try:
            starrocks_hook.run(summary_sql)
            print(f"Validation report stored in {VALIDATION_DATABASE}.{VALIDATION_RESULTS_TABLE}")
        except Exception as e:
            print(f"Error storing summary: {e}")
        
        # Check if we need to send alert
        if failure_percentage > MAX_FAILURE_PERCENTAGE:
            alert_message = f"""
            ⚠️ DATA QUALITY ALERT ⚠️
            
            Validation failure rate exceeded threshold!
            
            Details:
            - Failure rate: {failure_percentage:.2f}% (threshold: {MAX_FAILURE_PERCENTAGE}%)
            - Failed tables: {failed_tables}/{total_tables}
            - Failed checks: {failed_checks}/{total_checks}
            
            Please check the validation_results table for details.
            Validation Database: {VALIDATION_DATABASE}
            Source Database: {STAGING_DATABASE}
            """
            
            print("\n" + "!" * 80)
            print("ALERT: High validation failure rate detected!")
            print(alert_message)
            print("!" * 80)
            
        return {
            "validation_database": VALIDATION_DATABASE,
            "source_database": STAGING_DATABASE,
            "total_tables": total_tables,
            "failed_tables": failed_tables,
            "total_checks": total_checks,
            "failed_checks": failed_checks,
            "failure_percentage": failure_percentage,
            "alert_triggered": failure_percentage > MAX_FAILURE_PERCENTAGE
        }

    # Main DAG workflow
    init_db = initialize_validation_database()
    tables_to_validate = get_tables_to_validate()
    validation_results = validate_table_data.expand(table_metadata=tables_to_validate)
    report = generate_validation_report(validation_results)
    
    init_db >> tables_to_validate >> validation_results >> report

adventureworks_validation_dag()