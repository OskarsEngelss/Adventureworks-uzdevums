import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_feedback_category_data_into_dimfeedbackcategory_and_upload_to_starrocks",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimfeedbackcategory", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_feedback_category_data_into_dimfeedbackcategory_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_feedback_category_upsert WHERE FeedbackCategoryID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        # Note: Adjust the SELECT logic if these categories come from specific AdventureWorks tables 
        # (e.g., Production.ProductReview or a custom mapping table).
        load_staging_sql = f"""
            INSERT INTO adventureworks_staging.stg_dim_feedback_category_upsert
            SELECT 
                FeedbackCategoryID,
                CategoryName,
                CategoryDescription,
                CURRENT_DATE() AS SourceUpdateDate
            FROM (
                -- Replace this subquery with your actual source logic if different
                SELECT 1 as FeedbackCategoryID, 'Product Quality' as CategoryName, 'Feedback regarding build and material' as CategoryDescription
                UNION ALL SELECT 2, 'Delivery', 'Feedback regarding shipping speed and packaging'
                UNION ALL SELECT 3, 'Customer Service', 'Feedback regarding support interactions'
                UNION ALL SELECT 4, 'Price', 'Feedback regarding value and cost'
            ) src;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. UPSERT LOGIC (SCD Type 1)
        # Since StarRocks Primary Key tables support UPSERT via INSERT INTO...SELECT, 
        # we can handle updates and inserts in one go.
        
        upsert_sql = f"""
            INSERT INTO DimFeedbackCategory (
                FeedbackCategoryKey, 
                FeedbackCategoryID, 
                CategoryName, 
                CategoryDescription
            )
            SELECT 
                -- Generate a stable Surrogate Key based on the ID
                murmur_hash3_32(CAST(s.FeedbackCategoryID AS CHAR)),
                s.FeedbackCategoryID,
                s.CategoryName,
                s.CategoryDescription
            FROM adventureworks_staging.stg_dim_feedback_category_upsert s;
        """
        starrocks_hook.run(upsert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_feedback_category_data_into_dimfeedbackcategory_and_upload_to_starrocks()