import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_feedback_data_into_factcustomerfeedback_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "feedback", "cx"],
    default_args={
        "retries": 1, 
        "retry_delay": datetime.timedelta(minutes=5)
    }
)
def extract_transform_load_feedback_data_into_factcustomerfeedback_and_upload_to_starrocks():

    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()

        # --- STEP 1: QUARANTINE ---
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'FactCustomerFeedback',
                CAST(stg.productreviewid AS CHAR),
                CASE 
                    WHEN stg.rating NOT BETWEEN 1 AND 5 THEN 'Invalid Rating'
                    WHEN p.ProductKey IS NULL THEN 'Missing Product'
                END,
                1, 0, 0,
                json_object(
                    'product_id', CAST(stg.productid AS CHAR),
                    'rating', CAST(stg.rating AS CHAR),
                    'reviewer_name', stg.reviewername
                )
            FROM adventureworks_staging.stg_production_productreview stg
            LEFT JOIN adventureworks.DimProduct p 
                ON stg.productid = p.ProductID AND p.IsCurrent = 1
            WHERE stg.rating NOT BETWEEN 1 AND 5 OR p.ProductKey IS NULL;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: LOAD FACT TABLE ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks.FactCustomerFeedback;")

            load_sql = """
                INSERT INTO adventureworks.FactCustomerFeedback (
                    FeedbackID, FeedbackDateKey, CustomerKey, EmployeeKey, 
                    FeedbackCategoryKey, FeedbackScore, ComplaintCount, 
                    ResolutionTimeHours, CSATScore
                )
                WITH RankedInventory AS (
                    SELECT 
                        inv.productid,
                        dw.ManagerKey,
                        ROW_NUMBER() OVER(PARTITION BY inv.productid ORDER BY inv.quantity DESC) as inv_rank
                    FROM adventureworks_staging.stg_production_productinventory inv
                    JOIN adventureworks.DimWarehouse dw ON inv.locationid = dw.WarehouseID
                    WHERE dw.IsCurrent = 1
                )
                SELECT 
                    stg.productreviewid,
                    CAST(DATE_FORMAT(stg.reviewdate, '%Y%m%d') AS SIGNED),
                    COALESCE(c.CustomerKey, 0) AS CustomerKey,
                    COALESCE(ri.ManagerKey, 0) AS EmployeeKey, 
                    -- FIX: Mapping to 'Product Quality' which actually exists in your Dim
                    COALESCE(fcat.FeedbackCategoryKey, 0) AS FeedbackCategoryKey,
                    stg.rating,
                    CASE WHEN stg.rating <= 2 THEN 1 ELSE 0 END, 
                    24.00,
                    (stg.rating / 5.0) * 100
                FROM adventureworks_staging.stg_production_productreview stg
                LEFT JOIN adventureworks.DimProduct p 
                    ON stg.productid = p.ProductID AND p.IsCurrent = 1
                LEFT JOIN adventureworks.DimCustomer c 
                    ON stg.emailaddress = c.Email AND c.IsCurrent = 1
                LEFT JOIN adventureworks.DimFeedbackCategory fcat 
                    -- Matching against existing data found in your DESC/SELECT
                    ON fcat.CategoryName = 'Product Quality'
                LEFT JOIN RankedInventory ri 
                    ON stg.productid = ri.productid AND ri.inv_rank = 1
                WHERE stg.rating BETWEEN 1 AND 5
                  AND p.ProductKey IS NOT NULL;
            """
            starrocks_hook.run(load_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="FactCustomerFeedback",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during Fact load"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_load_feedback_data_into_factcustomerfeedback_and_upload_to_starrocks()