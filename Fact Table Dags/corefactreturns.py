import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_returns_data_into_factreturns_and_upload_to_starrocks",
    schedule=None, 
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "returns", "adventureworks"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    }
)
def extract_transform_load_returns_data_into_factreturns_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. QUARANTINE: Check for dimension integrity
        quarantine_sql = """
            INSERT INTO adventureworks_errors.fact_returns_errors (
                ReturnID, FailureReason, IsRecoverable, FailedData
            )
            SELECT 
                sr.returnid,
                CASE 
                    WHEN dp.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN dc.CustomerKey IS NULL THEN 'Missing CustomerKey'
                    WHEN sr.quantity <= 0 THEN 'Invalid Return Quantity'
                END as FailureReason,
                1 as IsRecoverable,
                json_object(
                    'returnid', CAST(sr.returnid AS VARCHAR),
                    'productid', CAST(sr.productid AS VARCHAR),
                    'customerid', CAST(sr.customerid AS VARCHAR)
                ) as FailedData
            FROM adventureworks_staging.stg_sales_returns_upsert sr
            LEFT JOIN DimProduct dp ON sr.productid = dp.productid AND dp.IsCurrent = TRUE
            LEFT JOIN DimCustomer dc ON sr.customerid = dc.customerid AND dc.IsCurrent = TRUE
            WHERE dp.ProductKey IS NULL OR dc.CustomerKey IS NULL OR sr.quantity <= 0;
        """
        starrocks_hook.run(quarantine_sql)

        # 2. LOAD FACT TABLE (Joining your DimReturnReason)
        load_fact_sql = """
            INSERT INTO FactReturns (
                ReturnID, ReturnDateKey, ProductKey, CustomerKey, StoreKey, 
                ReturnReasonKey, ReturnedQuantity, RefundAmount, RestockingFee
            )
            SELECT 
                sr.returnid,
                CAST(sr.returndate AS DATE),
                dp.ProductKey,
                dc.CustomerKey,
                COALESCE(ds.StoreKey, 0),
                COALESCE(drr.ReturnReasonKey, 0), -- Joining DimReturnReason
                sr.quantity,
                sr.refund_amount,
                sr.restocking_fee
            FROM adventureworks_staging.stg_sales_returns_upsert sr
            INNER JOIN DimProduct dp ON sr.productid = dp.productid AND dp.IsCurrent = TRUE
            INNER JOIN DimCustomer dc ON sr.customerid = dc.customerid AND dc.IsCurrent = TRUE
            LEFT JOIN adventureworks_staging.stg_sales_customer sc ON sr.customerid = sc.customerid
            LEFT JOIN DimStore ds ON sc.storeid = ds.storeid AND ds.IsCurrent = TRUE
            LEFT JOIN DimReturnReason drr ON sr.reasonid = drr.ReturnReasonID
            WHERE sr.quantity > 0;
        """
        starrocks_hook.run(load_fact_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_load_returns_data_into_factreturns_and_upload_to_starrocks()