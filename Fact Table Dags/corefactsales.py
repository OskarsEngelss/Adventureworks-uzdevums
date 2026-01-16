import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_sales_data_into_factsales_and_upload_to_starrocks",
    schedule="@daily",
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

        # 1. QUARANTINE: Using json_object with explicit VARCHAR casting for stability
        quarantine_sql = """
            INSERT INTO adventureworks_errors.fact_sales_errors (
                SalesOrderID, SalesOrderDetailID, FailureReason, IsRecoverable, FailedData
            )
            SELECT 
                s.salesorderid, 
                s.salesorderdetailid,
                CASE 
                    WHEN p.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN c.CustomerKey IS NULL THEN 'Missing CustomerKey'
                    WHEN (s.unitprice * CAST(s.orderqty AS DECIMAL)) < 0 THEN 'Negative Revenue'
                    WHEN h.orderdate > CURRENT_DATE() THEN 'Future Date'
                END as FailureReason,
                IF(p.ProductKey IS NULL OR c.CustomerKey IS NULL, 1, 0) as IsRecoverable,
                -- We use CAST AS VARCHAR inside json_object because it's the most compatible way
                json_object(
                    'salesorderid', CAST(s.salesorderid AS VARCHAR),
                    'productid', CAST(s.productid AS VARCHAR),
                    'revenue', CAST((s.unitprice * CAST(s.orderqty AS DECIMAL)) AS VARCHAR),
                    'orderdate', CAST(h.orderdate AS VARCHAR)
                ) as FailedData
            FROM adventureworks_staging.stg_sales_salesorderdetail s
            INNER JOIN adventureworks_staging.stg_sales_salesorderheader h 
                ON s.salesorderid = h.salesorderid
            LEFT JOIN DimProduct p ON s.productid = p.productid AND p.IsCurrent = TRUE
            LEFT JOIN DimCustomer c ON h.customerid = c.customerid AND c.IsCurrent = TRUE
            WHERE p.ProductKey IS NULL 
            OR c.CustomerKey IS NULL 
            OR (s.unitprice * CAST(s.orderqty AS DECIMAL)) < 0 
            OR h.orderdate > CURRENT_DATE();
        """
        starrocks_hook.run(quarantine_sql)

        # 2. LOAD FACT TABLE
        load_fact_sql = """
            INSERT INTO FactSales (
                SalesDateKey, CustomerKey, ProductKey, StoreKey, 
                EmployeeKey, SalesRevenue, QuantitySold, DiscountAmount
            )
            SELECT 
                CAST(h.orderdate AS DATE),
                c.CustomerKey,
                p.ProductKey,
                COALESCE(h.territoryid, 0),
                COALESCE(e.EmployeeKey, 0),
                (s.unitprice * CAST(s.orderqty AS DECIMAL)),
                CAST(s.orderqty AS INT),
                (s.unitprice * s.unitpricediscount * CAST(s.orderqty AS DECIMAL))
            FROM adventureworks_staging.stg_sales_salesorderdetail s
            INNER JOIN adventureworks_staging.stg_sales_salesorderheader h 
                ON s.salesorderid = h.salesorderid
            INNER JOIN DimProduct p ON s.productid = p.productid AND p.IsCurrent = TRUE
            INNER JOIN DimCustomer c ON h.customerid = c.customerid AND c.IsCurrent = TRUE
            LEFT JOIN DimEmployee e ON h.salespersonid = e.EmployeeID AND e.IsCurrent = TRUE
            WHERE (s.unitprice * CAST(s.orderqty AS DECIMAL)) >= 0 
            AND h.orderdate <= CURRENT_DATE();
        """
        starrocks_hook.run(load_fact_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_load_sales_data_into_factsales_and_upload_to_starrocks()