import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_region_data_into_dimregion_and_upload_to_starrocks",
    schedule="@daily",
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

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_region_upsert WHERE RegionID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = """
            INSERT INTO adventureworks_staging.stg_dim_region_upsert
            SELECT 
                RegionID,
                RegionName,
                Country,
                Continent,
                TimeZone,
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

        # 3. UPSERT Logic (SCD Type 1)
        upsert_sql = """
            INSERT INTO DimRegion (
                RegionKey, RegionID, RegionName, Country, Continent, TimeZone
            )
            SELECT 
                murmur_hash3_32(CAST(s.RegionID AS CHAR)) AS RegionKey,
                s.RegionID, 
                s.RegionName, 
                s.Country, 
                s.Continent, 
                s.TimeZone
            FROM adventureworks_staging.stg_dim_region_upsert s;
        """
        starrocks_hook.run(upsert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_region_data_into_dimregion_and_upload_to_starrocks()