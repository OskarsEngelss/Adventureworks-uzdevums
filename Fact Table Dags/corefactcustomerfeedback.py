import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_feedback_data_into_factcustomerfeedback_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "feedback", "cx"],
    default_args={"retries": 1, "retry_delay": datetime.timedelta(minutes=5)}
)
def extract_transform_load_feedback_data_into_factcustomerfeedback_and_upload_to_starrocks():

    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. QUARANTINE (ProductReview specific)
        quarantine_sql = """
            INSERT INTO adventureworks_errors.fact_customerfeedback_errors (
                SourceFeedbackID, FailureReason, IsRecoverable, FailedData
            )
            SELECT 
                stg.productreviewid,
                CASE 
                    WHEN stg.rating NOT BETWEEN 1 AND 5 THEN 'Invalid Rating'
                    WHEN p.ProductKey IS NULL THEN 'Missing Product'
                END as FailureReason,
                1 as IsRecoverable,
                json_object(
                    'product_id', CAST(stg.productid AS VARCHAR),
                    'rating', CAST(stg.rating AS VARCHAR),
                    'reviewer_name', stg.reviewername
                ) as FailedData
            FROM adventureworks_staging.stg_production_productreview stg
            LEFT JOIN DimProduct p ON stg.productid = p.ProductID
            WHERE stg.rating NOT BETWEEN 1 AND 5 OR p.ProductKey IS NULL;
        """
        starrocks_hook.run(quarantine_sql)

        # 2. LOAD (Using the correct CustomerName column)
        load_sql = """
            INSERT INTO FactCustomerFeedback (
                FeedbackID, FeedbackDateKey, CustomerKey, EmployeeKey, 
                FeedbackCategoryKey, FeedbackScore, ComplaintCount, 
                ResolutionTimeHours, CSATScore
            )
            SELECT 
                stg.productreviewid,
                CAST(stg.reviewdate AS DATE),
                COALESCE(c.CustomerKey, -1),
                -1, -- Mocking EmployeeKey (Handler)
                COALESCE(fcat.FeedbackCategoryKey, -1),
                stg.rating,
                CASE WHEN stg.rating <= 2 THEN 1 ELSE 0 END, 
                24.00,
                (stg.rating / 5.0) * 100
            FROM adventureworks_staging.stg_production_productreview stg
            LEFT JOIN DimProduct p ON stg.productid = p.ProductID
            -- Joining on the string name found in your DimCustomer
            LEFT JOIN DimCustomer c 
                ON stg.reviewername = c.CustomerName 
                AND c.IsCurrent = 1
            LEFT JOIN DimFeedbackCategory fcat 
                ON fcat.CategoryName = 'Product Review';
        """
        starrocks_hook.run(load_sql)
        
    synchronize_postgresql_to_starrocks()

extract_transform_load_feedback_data_into_factcustomerfeedback_and_upload_to_starrocks()