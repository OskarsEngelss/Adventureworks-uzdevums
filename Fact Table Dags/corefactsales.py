import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_sales_data_into_factsales_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "sales", "adventureworks"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    }
)
def extract_transform_load_sales_data_into_factsales_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()

        # --- STEP 1: QUARANTINE ---
        # Catching orphans (Sales without Products/Customers) or data anomalies
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'FactSales', 
                CONCAT(CAST(s.salesorderid AS CHAR), '-', CAST(s.salesorderdetailid AS CHAR)),
                CASE 
                    WHEN p.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN c.CustomerKey IS NULL THEN 'Missing CustomerKey'
                    WHEN (s.unitprice * CAST(s.orderqty AS DECIMAL)) < 0 THEN 'Negative Revenue'
                    WHEN h.orderdate > CURRENT_DATE() THEN 'Future Date'
                END,
                IF(p.ProductKey IS NULL OR c.CustomerKey IS NULL, 1, 0),
                0, 0,
                json_object(
                    'salesorderid', CAST(s.salesorderid AS CHAR),
                    'productid', CAST(s.productid AS CHAR),
                    'customerid', CAST(h.customerid AS CHAR),
                    'revenue', CAST((s.unitprice * CAST(s.orderqty AS DECIMAL)) AS CHAR)
                )
            FROM adventureworks_staging.stg_sales_salesorderdetail s
            INNER JOIN adventureworks_staging.stg_sales_salesorderheader h ON s.salesorderid = h.salesorderid
            LEFT JOIN adventureworks.DimProduct p ON s.productid = p.productid AND p.IsCurrent = 1
            LEFT JOIN adventureworks.DimCustomer c ON h.customerid = c.customerid AND c.IsCurrent = 1
            WHERE p.ProductKey IS NULL 
               OR c.CustomerKey IS NULL 
               OR (s.unitprice * CAST(s.orderqty AS DECIMAL)) < 0 
               OR h.orderdate > CURRENT_DATE();
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: LOAD FACT TABLE ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks.FactSales;")

            load_fact_sql = """
                INSERT INTO adventureworks.FactSales (
                    SalesDateKey, CustomerKey, ProductKey, StoreKey, 
                    EmployeeKey, SalesRevenue, QuantitySold, DiscountAmount
                )
                WITH FirstStoreVersion AS (
                    -- Handles historical sales where the order date precedes the SCD2 record
                    SELECT 
                        StoreID, 
                        StoreKey,
                        ROW_NUMBER() OVER(PARTITION BY StoreID ORDER BY ValidFromDate ASC) as rn
                    FROM adventureworks.DimStore
                )
                SELECT 
                    CAST(DATE_FORMAT(h.orderdate, '%Y%m%d') AS SIGNED),
                    c.CustomerKey,
                    p.ProductKey,
                    COALESCE(ds.StoreKey, fsv.StoreKey, 0), 
                    COALESCE(e.EmployeeKey, 0),
                    (s.unitprice * CAST(s.orderqty AS DECIMAL)),
                    CAST(s.orderqty AS INT),
                    (s.unitprice * s.unitpricediscount * CAST(s.orderqty AS DECIMAL))
                FROM adventureworks_staging.stg_sales_salesorderdetail s
                INNER JOIN adventureworks_staging.stg_sales_salesorderheader h 
                    ON s.salesorderid = h.salesorderid
                INNER JOIN adventureworks.DimProduct p ON s.productid = p.productid AND p.IsCurrent = TRUE
                INNER JOIN adventureworks.DimCustomer c ON h.customerid = c.customerid AND c.IsCurrent = TRUE
                LEFT JOIN adventureworks_staging.stg_sales_customer sc ON h.customerid = sc.customerid
                -- SCD2 Join (Temporal)
                LEFT JOIN adventureworks.DimStore ds ON sc.storeid = ds.StoreID 
                    AND CAST(h.orderdate AS DATE) >= ds.ValidFromDate 
                    AND (CAST(h.orderdate AS DATE) <= ds.ValidToDate OR ds.ValidToDate IS NULL)
                -- Fallback to first known version for old sales
                LEFT JOIN FirstStoreVersion fsv ON sc.storeid = fsv.StoreID AND fsv.rn = 1
                LEFT JOIN adventureworks.DimEmployee e ON h.salespersonid = e.EmployeeID AND e.IsCurrent = TRUE
                WHERE (s.unitprice * CAST(s.orderqty AS DECIMAL)) >= 0 
                  AND h.orderdate <= CURRENT_DATE();
            """
            starrocks_hook.run(load_fact_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="FactSales",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during FactSales load"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_load_sales_data_into_factsales_and_upload_to_starrocks()