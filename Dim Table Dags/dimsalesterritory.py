import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_sales_territory_data_into_dimsalesterritory_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimsalesterritory", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_sales_territory_data_into_dimsalesterritory_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()

        # --- STEP 1: QUARANTINE ---
        # Catching records with missing IDs or critical names
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimSalesTerritory',
                COALESCE(CAST(stg.territoryid AS CHAR), 'UNKNOWN'),
                'Missing TerritoryID or Name',
                1, 0, 0,
                json_object(
                    'territoryid', CAST(stg.territoryid AS CHAR), 
                    'name', stg.name
                )
            FROM adventureworks_staging.stg_sales_salesterritory stg
            WHERE stg.territoryid IS NULL OR stg.name IS NULL;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: CLEAN & TRANSFORM INTO STAGING ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks_staging.stg_dim_sales_territory_upsert;")

            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_sales_territory_upsert
                SELECT 
                    st.territoryid AS TerritoryID,
                    st.name AS TerritoryName,
                    st.`group` AS SalesRegion,
                    st.countryregioncode AS Country,
                    COALESCE(CONCAT(p.firstname, ' ', p.lastname), 'No Manager') AS Manager,
                    st.salesytd AS SalesTarget,
                    CURRENT_DATE() AS SourceUpdateDate
                FROM adventureworks_staging.stg_sales_salesterritory st
                LEFT JOIN adventureworks_staging.stg_sales_salesperson sp 
                    ON st.territoryid = sp.territoryid
                LEFT JOIN adventureworks_staging.stg_person_person p 
                    ON sp.businessentityid = p.businessentityid;
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: UPSERT Logic (SCD Type 1) ---
            # Truncating the final table to ensure a clean sync for this reference dimension
            starrocks_hook.run("TRUNCATE TABLE adventureworks.DimSalesTerritory;")

            upsert_sql = """
                INSERT INTO adventureworks.DimSalesTerritory (
                    TerritoryKey, TerritoryID, TerritoryName, SalesRegion, 
                    Country, Manager, SalesTarget
                )
                SELECT 
                    murmur_hash3_32(CAST(s.TerritoryID AS CHAR)) AS TerritoryKey,
                    s.TerritoryID, 
                    s.TerritoryName, 
                    s.SalesRegion, 
                    s.Country, 
                    s.Manager, 
                    s.SalesTarget
                FROM adventureworks_staging.stg_dim_sales_territory_upsert s;
            """
            starrocks_hook.run(upsert_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimSalesTerritory",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_sales_territory_data_into_dimsalesterritory_and_upload_to_starrocks()