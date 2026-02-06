import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

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
        proc_date = pendulum.now().to_date_string()

        # --- STEP 1: QUARANTINE ---
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'FactFinance',
                CAST(h.salesorderid AS CHAR),
                CASE 
                    WHEN c.CustomerKey IS NULL THEN 'Missing CustomerKey'
                    WHEN h.totaldue < 0 THEN 'Negative Invoice Amount'
                END,
                1, 0, 0,
                json_object(
                    'salesorderid', CAST(h.salesorderid AS CHAR),
                    'customerid', CAST(h.customerid AS CHAR),
                    'totaldue', CAST(h.totaldue AS CHAR)
                )
            FROM adventureworks_staging.stg_sales_salesorderheader h
            LEFT JOIN adventureworks.DimCustomer c 
                ON h.customerid = c.customerid AND c.IsCurrent = 1
            WHERE c.CustomerKey IS NULL OR h.totaldue < 0;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: LOAD FACT TABLE ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks.FactFinance;")

            load_fact_sql = """
                INSERT INTO adventureworks.FactFinance (
                    InvoiceID, InvoiceDateKey, CustomerKey, StoreKey, 
                    FinanceCategoryKey, InvoiceAmount, PaymentDelayDays, 
                    CreditUsage, InterestCharges
                )
                SELECT 
                    h.salesorderid,
                    CAST(DATE_FORMAT(h.orderdate, '%Y%m%d') AS SIGNED),
                    dc.CustomerKey,
                    COALESCE(ds.StoreKey, 0),
                    -- Use the key from the cross-joined subquery
                    dfc.FinanceCategoryKey,
                    h.totaldue,
                    DATEDIFF(COALESCE(h.shipdate, h.duedate), h.orderdate),
                    CASE 
                        WHEN dc.CreditLimit > 0 THEN (h.totaldue / dc.CreditLimit) * 100 
                        ELSE 0 
                    END,
                    (h.taxamt + h.freight)
                FROM adventureworks_staging.stg_sales_salesorderheader h
                INNER JOIN adventureworks.DimCustomer dc 
                    ON h.customerid = dc.customerid AND dc.IsCurrent = 1
                LEFT JOIN adventureworks_staging.stg_sales_customer sc 
                    ON h.customerid = sc.customerid
                LEFT JOIN adventureworks.DimStore ds 
                    ON sc.storeid = ds.storeid AND ds.IsCurrent = 1
                -- This ensures EVERY row in the header gets the 'Invoice' Key
                CROSS JOIN (
                    SELECT FinanceCategoryKey 
                    FROM adventureworks.DimFinanceCategory 
                    WHERE CategoryName = 'Invoice' 
                    LIMIT 1
                ) dfc
                WHERE h.totaldue >= 0;
            """
            starrocks_hook.run(load_fact_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="FactFinance",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during FactFinance transformation"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_load_finance_data_into_factfinance_and_upload_to_starrocks()