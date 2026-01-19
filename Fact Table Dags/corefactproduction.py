import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_production_data_into_factproduction_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "production", "adventureworks"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    }
)
def extract_transform_load_production_data_into_factproduction_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. QUARANTINE: Catch orphans, bad dates, or missing Employees
        quarantine_sql = """
            INSERT INTO adventureworks_errors.fact_production_errors (
                WorkOrderID, FailureReason, IsRecoverable, FailedData
            )
            SELECT 
                wo.workorderid,
                CASE 
                    WHEN p.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN e.EmployeeKey IS NULL THEN 'Missing EmployeeKey'
                    WHEN wo.StartDate > wo.EndDate THEN 'Invalid Dates: Start > End'
                END as FailureReason,
                1 as IsRecoverable,
                json_object(
                    'workorder_id', CAST(wo.workorderid AS VARCHAR),
                    'product_id', CAST(wo.productid AS VARCHAR),
                    'order_qty', CAST(wo.orderqty AS VARCHAR)
                ) as FailedData
            FROM adventureworks_staging.stg_production_workorder wo
            LEFT JOIN DimProduct p ON wo.productid = p.productid AND p.IsCurrent = TRUE
            -- Join routing to find the employee assigned
            LEFT JOIN (
                SELECT workorderid, MAX(operationsequence) as max_op 
                FROM adventureworks_staging.stg_production_workorderrouting 
                GROUP BY workorderid
            ) r_max ON wo.workorderid = r_max.workorderid
            LEFT JOIN adventureworks_staging.stg_production_workorderrouting wor 
                ON r_max.workorderid = wor.workorderid AND r_max.max_op = wor.operationsequence
            LEFT JOIN DimEmployee e ON wor.locationid = e.EmployeeID AND e.IsCurrent = TRUE
            WHERE p.ProductKey IS NULL 
               OR e.EmployeeKey IS NULL
               OR wo.startdate > wo.enddate;
        """
        starrocks_hook.run(quarantine_sql)

        # 2. LOAD FACT TABLE: Now with real EmployeeKeys
        starrocks_hook.run("TRUNCATE TABLE FactProduction;")

        load_fact_sql = """
            INSERT INTO FactProduction (
                ProductionDateKey, ProductKey, EmployeeKey, 
                ProductionTimeHours, UnitsProduced, ScrapRate, DefectCount
            )
            SELECT 
                CAST(wo.enddate AS DATE),
                p.ProductKey,
                e.EmployeeKey,
                ROUND(TIMESTAMPDIFF(SECOND, wo.startdate, wo.enddate) / 3600.0, 2),
                wo.orderqty,
                ROUND((CAST(wo.scrappedqty AS DECIMAL) / CAST(wo.orderqty AS DECIMAL)) * 100, 2),
                wo.scrappedqty
            FROM adventureworks_staging.stg_production_workorder wo
            INNER JOIN DimProduct p ON wo.productid = p.productid AND p.IsCurrent = TRUE
            -- Join routing to get the supervisor/employee from the last operation
            INNER JOIN (
                SELECT workorderid, MAX(operationsequence) as max_op 
                FROM adventureworks_staging.stg_production_workorderrouting 
                GROUP BY workorderid
            ) r_max ON wo.workorderid = r_max.workorderid
            INNER JOIN adventureworks_staging.stg_production_workorderrouting wor 
                ON r_max.workorderid = wor.workorderid AND r_max.max_op = wor.operationsequence
            INNER JOIN DimEmployee e ON wor.locationid = e.EmployeeID AND e.IsCurrent = TRUE
            WHERE wo.startdate <= wo.enddate;
        """
        starrocks_hook.run(load_fact_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_load_production_data_into_factproduction_and_upload_to_starrocks()