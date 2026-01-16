import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_return_reason_data_into_dimreturnreason_and_upload_to_starrocks",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimreturnreason", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_return_reason_data_into_dimreturnreason_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_return_reason_upsert WHERE ReturnReasonID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        # Using Static values as defined for AdventureWorks return analysis
        load_staging_sql = """
            INSERT INTO adventureworks_staging.stg_dim_return_reason_upsert
            SELECT 
                ReturnReasonID,
                ReturnReasonName,
                ReturnReasonDescription,
                CURRENT_DATE() AS SourceUpdateDate
            FROM (
                SELECT 1 as ReturnReasonID, 'Defective' as ReturnReasonName, 'Item does not function as intended' as ReturnReasonDescription
                UNION ALL SELECT 2, 'Wrong Item', 'Received a different product than ordered'
                UNION ALL SELECT 3, 'Changed Mind', 'Customer no longer wants the product'
                UNION ALL SELECT 4, 'Damaged', 'Product arrived with physical damage'
            ) src;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. UPSERT Logic (SCD Type 1 - Overwrite)
        upsert_sql = """
            INSERT INTO DimReturnReason (
                ReturnReasonKey, ReturnReasonID, ReturnReasonName, 
                ReturnReasonDescription
            )
            SELECT 
                murmur_hash3_32(CAST(s.ReturnReasonID AS CHAR)) AS ReturnReasonKey,
                s.ReturnReasonID, 
                s.ReturnReasonName, 
                s.ReturnReasonDescription
            FROM adventureworks_staging.stg_dim_return_reason_upsert s;
        """
        starrocks_hook.run(upsert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_return_reason_data_into_dimreturnreason_and_upload_to_starrocks()