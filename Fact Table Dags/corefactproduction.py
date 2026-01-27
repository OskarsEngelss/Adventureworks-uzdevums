import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

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
        proc_date = pendulum.now().to_date_string()

        # --- STEP 1: QUARANTINE ---
        # Identifying orphans or logic violations (Start > End)
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'FactProduction',
                CAST(wo.workorderid AS CHAR),
                CASE 
                    WHEN p.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN e.EmployeeKey IS NULL THEN 'Missing EmployeeKey'
                    WHEN wo.startdate > wo.enddate THEN 'Invalid Dates: Start > End'
                END,
                1, 0, 0,
                json_object(
                    'workorder_id', CAST(wo.workorderid AS CHAR),
                    'product_id', CAST(wo.productid AS CHAR),
                    'order_qty', CAST(wo.orderqty AS CHAR)
                )
            FROM adventureworks_staging.stg_production_workorder wo
            LEFT JOIN adventureworks.DimProduct p 
                ON wo.productid = p.productid AND p.IsCurrent = 1
            LEFT JOIN (
                SELECT workorderid, MAX(CAST(operationsequence AS UNSIGNED)) as max_op 
                FROM adventureworks_staging.stg_production_workorderrouting 
                GROUP BY workorderid
            ) r_max ON wo.workorderid = r_max.workorderid
            LEFT JOIN adventureworks_staging.stg_production_workorderrouting wor 
                ON r_max.workorderid = wor.workorderid 
                AND r_max.max_op = CAST(wor.operationsequence AS UNSIGNED)
            LEFT JOIN adventureworks.DimEmployee e 
                ON CAST(wor.locationid AS SIGNED) = e.EmployeeID AND e.IsCurrent = 1
            WHERE p.ProductKey IS NULL 
               OR e.EmployeeKey IS NULL
               OR wo.startdate > wo.enddate;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: LOAD FACT TABLE ---
            # We truncate and reload to ensure we capture the most up-to-date Employee assignments
            starrocks_hook.run("TRUNCATE TABLE adventureworks.FactProduction;")

            load_fact_sql = """
                INSERT INTO adventureworks.FactProduction (
                    ProductionDateKey, ProductKey, EmployeeKey, 
                    ProductionTimeHours, UnitsProduced, ScrapRate, DefectCount
                )
                SELECT 
                    CAST(wo.enddate AS DATE),
                    p.ProductKey,
                    e.EmployeeKey,
                    ROUND(TIMESTAMPDIFF(SECOND, wo.startdate, wo.enddate) / 3600.0, 2),
                    wo.orderqty,
                    ROUND((CAST(wo.scrappedqty AS DECIMAL) / NULLIF(CAST(wo.orderqty AS DECIMAL), 0)) * 100, 2),
                    CAST(wo.scrappedqty AS INT)
                FROM adventureworks_staging.stg_production_workorder wo
                INNER JOIN adventureworks.DimProduct p 
                    ON wo.productid = p.productid AND p.IsCurrent = 1
                INNER JOIN (
                    SELECT workorderid, MAX(CAST(operationsequence AS UNSIGNED)) as max_op 
                    FROM adventureworks_staging.stg_production_workorderrouting 
                    GROUP BY workorderid
                ) r_max ON wo.workorderid = r_max.workorderid
                INNER JOIN adventureworks_staging.stg_production_workorderrouting wor 
                    ON r_max.workorderid = wor.workorderid 
                    AND r_max.max_op = CAST(wor.operationsequence AS UNSIGNED)
                INNER JOIN adventureworks.DimEmployee e 
                    ON CAST(wor.locationid AS SIGNED) = e.EmployeeID AND e.IsCurrent = 1
                WHERE wo.startdate <= wo.enddate;
            """
            starrocks_hook.run(load_fact_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="FactProduction",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during FactProduction transformation"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_load_production_data_into_factproduction_and_upload_to_starrocks()