import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

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

        # 1. QUARANTINE: Catch missing EmployeeKeys or TerritoryKeys before loading
        quarantine_sql = """
            INSERT INTO adventureworks_errors.fact_employeesales_errors (
                SalesOrderID, FailureReason, IsRecoverable, FailedData
            )
            SELECT 
                soh.salesorderid,
                CASE 
                    WHEN e.EmployeeKey IS NULL THEN 'Missing EmployeeKey'
                    WHEN st.TerritoryKey IS NULL THEN 'Missing TerritoryKey'
                    WHEN (soh.totaldue < 0 OR soh.totaldue IS NULL) THEN 'Invalid Sales Amount'
                END as FailureReason,
                1 as IsRecoverable,
                json_object(
                    'sales_person_id', CAST(soh.salespersonid AS VARCHAR),
                    'territory_id', CAST(soh.territoryid AS VARCHAR),
                    'total_due', CAST(soh.totaldue AS VARCHAR)
                ) as FailedData
            FROM adventureworks_staging.stg_sales_salesorderheader soh
            LEFT JOIN DimEmployee e ON soh.salespersonid = e.EmployeeID AND e.IsCurrent = 1
            LEFT JOIN DimSalesTerritory st ON soh.territoryid = st.TerritoryID
            WHERE (e.EmployeeKey IS NULL AND soh.salespersonid IS NOT NULL)
               OR (st.TerritoryKey IS NULL AND soh.territoryid IS NOT NULL)
               OR (soh.totaldue < 0 OR soh.totaldue IS NULL);
        """
        starrocks_hook.run(quarantine_sql)

        # 2. LOAD FACT TABLE
        load_fact_sql = """
            INSERT INTO FactEmployeeSales (
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
                GROUP BY 1, 2, 3
            )
            SELECT 
                da.SalesDate,
                e.EmployeeKey,
                -1,
                COALESCE(st.TerritoryKey, -1), -- Changed to TerritoryKey
                da.DailyTotal,
                COALESCE(dq.SalesQuota / 90, 0),
                da.ContactCount
            FROM DailyAgg da
            INNER JOIN DimEmployee e ON da.salespersonid = e.EmployeeID AND e.IsCurrent = 1
            LEFT JOIN DimSalesTerritory st ON da.territoryid = st.TerritoryID
            LEFT JOIN adventureworks_staging.stg_sales_salespersonquotahistory dq 
                ON da.salespersonid = dq.businessentityid 
                AND da.SalesDate >= dq.quotadate 
                AND da.SalesDate < DATE_ADD(dq.quotadate, INTERVAL 3 MONTH);
        """
        starrocks_hook.run(load_fact_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_load_sales_data_into_factemployeesales_and_upload_to_starrocks()