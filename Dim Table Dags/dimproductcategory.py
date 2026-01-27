import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_product_category_data_into_dimproductcategory_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimproductcategory", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_product_category_data_into_dimproductcategory_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()

        # --- STEP 1: QUARANTINE (Identify Bad Data) ---
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimProductCategory',
                COALESCE(CAST(stg.productcategoryid AS CHAR), 'MISSING'),
                'Missing CategoryID or Name',
                1, 0, 0,
                json_object('categoryid', stg.productcategoryid, 'name', stg.name)
            FROM adventureworks_staging.stg_production_productcategory stg
            WHERE stg.productcategoryid IS NULL OR stg.name IS NULL;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: DYNAMIC STAGING LOAD ---
            # Clean intermediate table
            starrocks_hook.run("TRUNCATE TABLE adventureworks_staging.stg_dim_product_category_upsert;")

            # Load ONLY valid records from source staging
            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_product_category_upsert (
                    ProductCategoryID, CategoryName, CategoryDescription, SourceUpdateDate
                )
                SELECT 
                    productcategoryid, 
                    name, 
                    name, -- Using name as description if source doesn't have a desc col
                    CURRENT_DATE()
                FROM adventureworks_staging.stg_production_productcategory
                WHERE productcategoryid IS NOT NULL AND name IS NOT NULL;
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: SCD TYPE 1 UPSERT (Delete + Insert) ---
            # Remove existing categories so we can re-insert updated values
            starrocks_hook.run("""
                DELETE FROM adventureworks.DimProductCategory 
                WHERE ProductCategoryID IN (SELECT ProductCategoryID FROM adventureworks_staging.stg_dim_product_category_upsert);
            """)

            # INSERT with SURROGATE KEY generation
            upsert_sql = """
                INSERT INTO adventureworks.DimProductCategory (
                    ProductCategoryKey, ProductCategoryID, CategoryName, CategoryDescription
                )
                SELECT 
                    murmur_hash3_32(CAST(s.ProductCategoryID AS CHAR)), -- PROPER SURROGATE KEY
                    s.ProductCategoryID, 
                    s.CategoryName, 
                    s.CategoryDescription
                FROM adventureworks_staging.stg_dim_product_category_upsert s;
            """
            starrocks_hook.run(upsert_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimProductCategory",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_product_category_data_into_dimproductcategory_and_upload_to_starrocks()