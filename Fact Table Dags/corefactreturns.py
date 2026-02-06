import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

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
        proc_date = pendulum.now().to_date_string()

        # 1. QUARANTINE: Check simulation integrity
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'FactReturns',
                CAST(sr.returnid AS CHAR),
                CASE 
                    WHEN dp.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN dc.CustomerKey IS NULL THEN 'Missing CustomerKey'
                    WHEN sr.quantity <= 0 THEN 'Invalid Return Quantity'
                END,
                1, 0, 0,
                json_object(
                    'returnid', CAST(sr.returnid AS CHAR),
                    'productid', CAST(sr.productid AS CHAR),
                    'customerid', CAST(sr.customerid AS CHAR)
                )
            FROM adventureworks_staging.stg_sales_returns_upsert sr
            LEFT JOIN adventureworks.DimProduct dp ON sr.productid = dp.productid AND dp.IsCurrent = 1
            LEFT JOIN adventureworks.DimCustomer dc ON sr.customerid = dc.customerid AND dc.IsCurrent = 1
            WHERE dp.ProductKey IS NULL OR dc.CustomerKey IS NULL OR sr.quantity <= 0;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # 2. RELOAD FACT TABLE
            starrocks_hook.run("TRUNCATE TABLE adventureworks.FactReturns;")

            load_fact_sql = """
                INSERT INTO adventureworks.FactReturns (
                    ReturnID, ReturnDateKey, ProductKey, CustomerKey, StoreKey, 
                    ReturnReasonKey, ReturnedQuantity, RefundAmount, RestockingFee
                )
                WITH SimulatedReturns AS (
                    SELECT 
                        (sod.salesorderid * 1000 + sod.salesorderdetailid) as ReturnID,
                        CAST(soh.orderdate + INTERVAL (FLOOR(RAND() * 45) + 15) DAY AS DATE) as ReturnDate,
                        sod.productid,
                        soh.customerid,
                        COALESCE(sc.storeid, 0) as storeid,
                        FLOOR(RAND() * 4) + 1 as SimulatedReasonID, 
                        CAST(sod.orderqty AS DECIMAL) as Qty,
                        sod.unitprice * CAST(sod.orderqty AS DECIMAL) as PotentialRefund
                    FROM adventureworks_staging.stg_sales_salesorderheader soh
                    INNER JOIN adventureworks_staging.stg_sales_salesorderdetail sod 
                        ON soh.salesorderid = sod.salesorderid
                    LEFT JOIN adventureworks_staging.stg_sales_customer sc 
                        ON soh.customerid = sc.customerid
                    WHERE soh.status = 5 -- 'Shipped' orders only
                      AND RAND() < 0.08  -- 8% return rate simulation
                )
                SELECT 
                    sr.ReturnID,
                    CAST(DATE_FORMAT(sr.ReturnDate, '%Y%m%d') AS SIGNED),
                    dp.ProductKey,
                    dc.CustomerKey,
                    COALESCE(ds.StoreKey, 0),
                    COALESCE(drr.ReturnReasonKey, -1848669458), 
                    sr.Qty,
                    CAST(sr.PotentialRefund AS DECIMAL(18,2)),
                    CAST(CASE WHEN RAND() < 0.3 THEN sr.PotentialRefund * 0.15 ELSE 0 END AS DECIMAL(10,2))
                FROM SimulatedReturns sr
                INNER JOIN adventureworks.DimProduct dp ON sr.productid = dp.productid AND dp.IsCurrent = TRUE
                INNER JOIN adventureworks.DimCustomer dc ON sr.customerid = dc.customerid AND dc.IsCurrent = TRUE
                LEFT JOIN adventureworks.DimStore ds ON sr.storeid = ds.storeid AND ds.IsCurrent = TRUE
                LEFT JOIN adventureworks.DimReturnReason drr ON sr.SimulatedReasonID = drr.ReturnReasonID;
            """
            starrocks_hook.run(load_fact_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="FactReturns",
                natural_key="SIM_BATCH_" + proc_date,
                error=e,
                failed_data="Simulation failure during FactReturns generation"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_load_returns_data_into_factreturns_and_upload_to_starrocks()