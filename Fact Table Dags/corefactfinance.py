import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_finance_data_into_factfinance_and_upload_to_starrocks",
    schedule=None, 
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "finance", "adventureworks"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    }
)
def extract_transform_load_finance_data_into_factfinance_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. QUARANTINE: Checking for missing Customer or Date integrity
        quarantine_sql = """
            INSERT INTO adventureworks_errors.fact_finance_errors (
                InvoiceID, FailureReason, IsRecoverable, FailedData
            )
            SELECT 
                h.salesorderid,
                CASE 
                    WHEN c.CustomerKey IS NULL THEN 'Missing CustomerKey'
                    WHEN h.totaldue < 0 THEN 'Negative Invoice Amount'
                END as FailureReason,
                1 as IsRecoverable,
                json_object(
                    'salesorderid', CAST(h.salesorderid AS VARCHAR),
                    'customerid', CAST(h.customerid AS VARCHAR),
                    'totaldue', CAST(h.totaldue AS VARCHAR)
                ) as FailedData
            FROM adventureworks_staging.stg_sales_salesorderheader h
            LEFT JOIN DimCustomer c ON h.customerid = c.customerid AND c.IsCurrent = TRUE
            WHERE c.CustomerKey IS NULL OR h.totaldue < 0;
        """
        starrocks_hook.run(quarantine_sql)

        # 2. LOAD FACT TABLE (Pure Join-Based)
        # Logic:
        # - StoreKey: Joined via stg_sales_customer to resolve the StoreID to a DimStore record.
        # - FinanceCategoryKey: Joined via DimFinanceCategory using the onlineorderflag.
        load_fact_sql = """
            INSERT INTO FactFinance (
                InvoiceID, InvoiceDateKey, CustomerKey, StoreKey, 
                FinanceCategoryKey, InvoiceAmount, PaymentDelayDays, 
                CreditUsage, InterestCharges
            )
            SELECT 
                h.salesorderid,
                CAST(h.orderdate AS DATE),
                dc.CustomerKey,
                COALESCE(ds.StoreKey, 0), -- 0 represents 'No Store/Direct'
                dfc.FinanceCategoryKey,
                h.totaldue,
                DATEDIFF(COALESCE(h.shipdate, h.duedate), h.orderdate),
                CASE 
                    WHEN dc.CreditLimit > 0 THEN (h.totaldue / dc.CreditLimit) * 100 
                    ELSE 0 
                END,
                (h.taxamt + h.freight)
            FROM adventureworks_staging.stg_sales_salesorderheader h
            -- Resolve Customer
            INNER JOIN DimCustomer dc ON h.customerid = dc.customerid AND dc.IsCurrent = TRUE
            -- Resolve Store (Header -> Customer Staging -> DimStore)
            LEFT JOIN adventureworks_staging.stg_sales_customer sc ON h.customerid = sc.customerid
            LEFT JOIN DimStore ds ON sc.storeid = ds.storeid AND ds.IsCurrent = TRUE
            -- Resolve Finance Category (Using onlineorderflag as the ID)
            LEFT JOIN DimFinanceCategory dfc ON h.onlineorderflag = dfc.FinanceCategoryID
            WHERE h.totaldue >= 0;
        """
        starrocks_hook.run(load_fact_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_load_finance_data_into_factfinance_and_upload_to_starrocks()