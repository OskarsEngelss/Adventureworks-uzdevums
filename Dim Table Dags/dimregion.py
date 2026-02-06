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

        try:
            # --- STEP 1: QUARANTINE ---
            check_sql = "SELECT COUNT(*) FROM adventureworks_staging.stg_sales_salesterritory"
            count = starrocks_hook.get_first(check_sql)[0]
            
            if count == 0:
                raise ValueError("Source staging table stg_sales_salesterritory is empty. Aborting load.")

            # --- STEP 2: TRANSFORM & LOAD TO STAGING ---
            starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_region_upsert WHERE RegionID IS NOT NULL;")

            # Joining with countryregion to get full names + Mapping TimeZones
            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_region_upsert (
                    RegionID, RegionName, Country, Continent, TimeZone, SourceUpdateDate
                )
                SELECT 
                    t.territoryid AS RegionID,
                    t.name AS RegionName,
                    c.name AS Country,
                    t.`group` AS Continent,
                    CASE 
                        WHEN t.name = 'Northwest' THEN 'PST'
                        WHEN t.name = 'Southwest' THEN 'MST'
                        WHEN t.name = 'Central' THEN 'CST'
                        WHEN t.name IN ('Northeast', 'Southeast', 'Canada') THEN 'EST'
                        WHEN t.`group` = 'Europe' THEN 'CET'
                        WHEN t.name = 'United Kingdom' THEN 'GMT'
                        WHEN t.name = 'Australia' THEN 'AEST'
                        ELSE 'UTC'
                    END AS TimeZone,
                    CURRENT_DATE() AS SourceUpdateDate
                FROM adventureworks_staging.stg_sales_salesterritory t
                LEFT JOIN adventureworks_staging.stg_person_countryregion c 
                    ON t.countryregioncode = c.countryregioncode
                UNION ALL
                SELECT 0, 'Online', 'Online', 'Online', 'UTC', CURRENT_DATE();
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: UPSERT Logic (SCD Type 1) ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks.DimRegion;")

            upsert_sql = """
                INSERT INTO adventureworks.DimRegion (
                    RegionKey, RegionID, RegionName, Country, Continent, TimeZone
                )
                SELECT 
                    CASE 
                        WHEN s.RegionID = 0 THEN 0 
                        ELSE murmur_hash3_32(CAST(s.RegionID AS CHAR)) 
                    END AS RegionKey,
                    s.RegionID, s.RegionName, s.Country, s.Continent, s.TimeZone
                FROM adventureworks_staging.stg_dim_region_upsert s;
            """
            starrocks_hook.run(upsert_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimRegion",
                natural_key="BATCH_" + proc_date,
                error=str(e),
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_region_data_into_dimregion_and_upload_to_starrocks()