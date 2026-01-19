import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_product_category_data_into_dimproductcategory_and_upload_to_starrocks",
    schedule=None, #schedule="@daily",
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

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_product_category_upsert WHERE ProductCategoryID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = """
            INSERT INTO adventureworks_staging.stg_dim_product_category_upsert
            SELECT 
                ProductCategoryID,
                CategoryName,
                CategoryDescription,
                CURRENT_DATE() AS SourceUpdateDate
            FROM (
                SELECT 1 as ProductCategoryID, 'Bikes' as CategoryName, 'Mountain, Road, and Touring bikes' as CategoryDescription
                UNION ALL SELECT 2, 'Components', 'Replacement parts like handlebars, pedals, and frames' as CategoryDescription
                UNION ALL SELECT 3, 'Clothing', 'Jerseys, shorts, and seasonal riding gear' as CategoryDescription
                UNION ALL SELECT 4, 'Accessories', 'Helmets, pumps, tires, and bottles' as CategoryDescription
            ) src;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. UPSERT Logic (SCD Type 1)
        upsert_sql = """
            INSERT INTO DimProductCategory (
                ProductCategoryKey, ProductCategoryID, CategoryName, CategoryDescription
            )
            SELECT 
                murmur_hash3_32(CAST(s.ProductCategoryID AS CHAR)) AS ProductCategoryKey,
                s.ProductCategoryID, 
                s.CategoryName, 
                s.CategoryDescription
            FROM adventureworks_staging.stg_dim_product_category_upsert s;
        """
        starrocks_hook.run(upsert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_product_category_data_into_dimproductcategory_and_upload_to_starrocks()