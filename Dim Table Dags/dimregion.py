import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_region_data_into_dimregion_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimregion", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_region_data_into_dimregion_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()

        # --- STEP 1: QUARANTINE (Check against the manual source) ---
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimRegion',
                COALESCE(CAST(stg.territoryid AS CHAR), 'UNKNOWN'),
                'Missing Region Data',
                1, 0, 0,
                json_object(
                    'regionid', CAST(stg.territoryid AS CHAR), 
                    'name', stg.name
                )
            FROM adventureworks_staging.stg_sales_salesterritory stg
            WHERE stg.territoryid IS NULL OR stg.name IS NULL;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: TRANSFORM & LOAD TO STAGING ---
            # Using your manual enrichment logic to add Continent and TimeZone
            starrocks_hook.run("TRUNCATE TABLE adventureworks_staging.stg_dim_region_upsert;")
            
            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_region_upsert
                SELECT 
                    RegionID, RegionName, Country, Continent, TimeZone, 
                    CURRENT_DATE() AS SourceUpdateDate
                FROM (
                    SELECT 1 as RegionID, 'Northwest' as RegionName, 'United States' as Country, 'North America' as Continent, 'PST' as TimeZone
                    UNION ALL SELECT 2, 'Northeast', 'United States', 'North America', 'EST'
                    UNION ALL SELECT 3, 'Central', 'United States', 'North America', 'CST'
                    UNION ALL SELECT 4, 'Southwest', 'United States', 'North America', 'MST'
                    UNION ALL SELECT 5, 'Southeast', 'United States', 'North America', 'EST'
                    UNION ALL SELECT 6, 'Canada', 'Canada', 'North America', 'EST'
                    UNION ALL SELECT 7, 'France', 'France', 'Europe', 'CET'
                    UNION ALL SELECT 8, 'Germany', 'Germany', 'Europe', 'CET'
                    UNION ALL SELECT 9, 'Australia', 'Australia', 'Pacific', 'AEST'
                    UNION ALL SELECT 10, 'United Kingdom', 'United Kingdom', 'Europe', 'GMT'
                ) src;
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: UPSERT Logic (SCD Type 1) ---
            # We truncate DimRegion first to ensure it's a clean reference table
            starrocks_hook.run("TRUNCATE TABLE adventureworks.DimRegion;")

            upsert_sql = """
                INSERT INTO adventureworks.DimRegion (
                    RegionKey, RegionID, RegionName, Country, Continent, TimeZone
                )
                SELECT 
                    murmur_hash3_32(CAST(s.RegionID AS CHAR)) AS RegionKey,
                    s.RegionID, s.RegionName, s.Country, s.Continent, s.TimeZone
                FROM adventureworks_staging.stg_dim_region_upsert s;
            """
            starrocks_hook.run(upsert_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimRegion",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_region_data_into_dimregion_and_upload_to_starrocks()