import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_aging_tier_data_into_dimagingtier_and_upload_to_starrocks",
    schedule=None, #schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimagingtier", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_aging_tier_data_into_dimagingtier_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_aging_tier_upsert WHERE AgingTierID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = """
            INSERT INTO adventureworks_staging.stg_dim_aging_tier_upsert
            SELECT 
                AgingTierID,
                AgingTierName,
                MinAgingDays,
                MaxAgingDays,
                CURRENT_DATE() AS SourceUpdateDate
            FROM (
                SELECT 1 as AgingTierID, 'Fresh (0-30 days)' as AgingTierName, 0 as MinAgingDays, 30 as MaxAgingDays
                UNION ALL SELECT 2, 'Aged (31-90 days)', 31, 90
                UNION ALL SELECT 3, 'Very Aged (91-180 days)', 91, 180
                UNION ALL SELECT 4, 'Obsolete (180+ days)', 181, 9999
            ) src;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. UPSERT Logic (SCD Type 1)
        upsert_sql = """
            INSERT INTO DimAgingTier (
                AgingTierKey, AgingTierID, AgingTierName, MinAgingDays, MaxAgingDays
            )
            SELECT 
                murmur_hash3_32(CAST(s.AgingTierID AS CHAR)) AS AgingTierKey,
                s.AgingTierID, 
                s.AgingTierName, 
                s.MinAgingDays, 
                s.MaxAgingDays
            FROM adventureworks_staging.stg_dim_aging_tier_upsert s;
        """
        starrocks_hook.run(upsert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_aging_tier_data_into_dimagingtier_and_upload_to_starrocks()