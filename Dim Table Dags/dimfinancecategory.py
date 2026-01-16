import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_finance_category_data_into_dimfinancecategory_and_upload_to_starrocks",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimfinancecategory", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_finance_category_data_into_dimfinancecategory_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_finance_category_upsert WHERE FinanceCategoryID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = """
            INSERT INTO adventureworks_staging.stg_dim_finance_category_upsert
            SELECT 
                FinanceCategoryID,
                CategoryName,
                CategoryDescription,
                CURRENT_DATE() AS SourceUpdateDate
            FROM (
                SELECT 1 as FinanceCategoryID, 'Invoice' as CategoryName, 'Standard billing document for sales' as CategoryDescription
                UNION ALL SELECT 2, 'Payment', 'Cash or credit received from customer' as CategoryDescription
                UNION ALL SELECT 3, 'Credit Memo', 'Reduction in amount owed by customer' as CategoryDescription
                UNION ALL SELECT 4, 'Adjustment', 'Correction to an existing financial record' as CategoryDescription
                UNION ALL SELECT 5, 'Refund', 'Amount returned to customer' as CategoryDescription
            ) src;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. UPSERT Logic (SCD Type 1)
        upsert_sql = """
            INSERT INTO DimFinanceCategory (
                FinanceCategoryKey, FinanceCategoryID, CategoryName, CategoryDescription
            )
            SELECT 
                murmur_hash3_32(CAST(s.FinanceCategoryID AS CHAR)) AS FinanceCategoryKey,
                s.FinanceCategoryID, 
                s.CategoryName, 
                s.CategoryDescription
            FROM adventureworks_staging.stg_dim_finance_category_upsert s;
        """
        starrocks_hook.run(upsert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_finance_category_data_into_dimfinancecategory_and_upload_to_starrocks()