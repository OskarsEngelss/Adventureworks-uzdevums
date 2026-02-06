import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_sales_data_into_factemployeesales_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "sales", "adventureworks"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    }
)
def extract_transform_load_sales_data_into_factemployeesales_and_upload_to_starrocks():

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
                'FactEmployeeSales',
                CAST(soh.salesorderid AS CHAR),
                CASE 
                    WHEN e.EmployeeKey IS NULL THEN 'Missing EmployeeKey'
                    WHEN st.TerritoryKey IS NULL AND soh.territoryid IS NOT NULL THEN 'Missing TerritoryKey'
                    WHEN soh.totaldue < 0 OR soh.totaldue IS NULL THEN 'Invalid Sales Amount'
                END,
                1, 0, 0,
                json_object(
                    'salespersonid', CAST(soh.salespersonid AS CHAR),
                    'territoryid', CAST(soh.territoryid AS CHAR),
                    'totaldue', CAST(soh.totaldue AS CHAR)
                )
            FROM adventureworks_staging.stg_sales_salesorderheader soh
            LEFT JOIN adventureworks.DimEmployee e 
                ON soh.salespersonid = e.EmployeeID AND e.IsCurrent = 1
            LEFT JOIN adventureworks.DimSalesTerritory st 
                ON soh.territoryid = st.TerritoryID
            WHERE (e.EmployeeKey IS NULL AND soh.salespersonid IS NOT NULL)
               OR (st.TerritoryKey IS NULL AND soh.territoryid IS NOT NULL)
               OR (soh.totaldue < 0 OR soh.totaldue IS NULL);
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: LOAD FACT TABLE ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks.FactEmployeeSales;")

            load_fact_sql = """
                INSERT INTO adventureworks.FactEmployeeSales (
                    SalesDateKey, EmployeeKey, StoreKey, 
                    SalesTerritoryKey, SalesAmount, SalesTarget, CustomerContactsCount
                )
                WITH DailyAgg AS (
                    SELECT 
                        CAST(orderdate AS DATE) as SalesDate,
                        salespersonid,
                        territoryid,
                        SUM(totaldue) as DailyTotal,
                        COUNT(salesorderid) as ContactCount
                    FROM adventureworks_staging.stg_sales_salesorderheader
                    WHERE salespersonid IS NOT NULL
                      AND totaldue >= 0 
                    GROUP BY 1, 2, 3
                )
                SELECT 
                    CAST(DATE_FORMAT(da.SalesDate, '%Y%m%d') AS SIGNED),
                    e.EmployeeKey,
                    -- JOIN to DimStore using the StoreID we just fixed in DimEmployee
                    COALESCE(ds.StoreKey, 0) AS StoreKey, 
                    COALESCE(st.TerritoryKey, 0) AS SalesTerritoryKey,
                    da.DailyTotal,
                    COALESCE(dq.salesquota / 90, 0) AS SalesTarget, 
                    da.ContactCount
                FROM DailyAgg da
                INNER JOIN adventureworks.DimEmployee e 
                    ON da.salespersonid = e.EmployeeID 
                    AND e.IsCurrent = 1
                -- Bridge to DimStore to get the surrogate StoreKey
                LEFT JOIN adventureworks.DimStore ds 
                    ON e.StoreID = ds.StoreID 
                    AND ds.IsCurrent = 1
                LEFT JOIN adventureworks.DimSalesTerritory st 
                    ON da.territoryid = st.TerritoryID
                LEFT JOIN adventureworks_staging.stg_sales_salespersonquotahistory dq 
                    ON da.salespersonid = dq.businessentityid 
                    AND da.SalesDate >= CAST(dq.quotadate AS DATE)
                    AND da.SalesDate < DATE_ADD(CAST(dq.quotadate AS DATE), INTERVAL 3 MONTH);
            """
            starrocks_hook.run(load_fact_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="FactEmployeeSales",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during FactEmployeeSales transformation"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_load_sales_data_into_factemployeesales_and_upload_to_starrocks()