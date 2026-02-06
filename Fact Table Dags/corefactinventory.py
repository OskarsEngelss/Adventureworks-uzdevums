import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_inventory_data_into_factinventory_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "inventory", "adventureworks"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    }
)
def extract_transform_load_inventory_data_into_factinventory_and_upload_to_starrocks():
    
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
                'FactInventory',
                CONCAT(CAST(s.productid AS CHAR), '-', CAST(s.locationid AS CHAR)),
                CASE 
                    WHEN p.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN w.WarehouseKey IS NULL THEN 'Missing WarehouseKey'
                    WHEN CAST(s.quantity AS SIGNED) < 0 THEN 'Negative Quantity'
                END,
                1, 0, 0,
                json_object(
                    'product_id', CAST(s.productid AS CHAR),
                    'location_id', CAST(s.locationid AS CHAR),
                    'quantity', CAST(s.quantity AS CHAR)
                )
            FROM adventureworks_staging.stg_production_productinventory s
            LEFT JOIN adventureworks.DimProduct p 
                ON s.productid = p.productid AND p.IsCurrent = 1
            LEFT JOIN adventureworks.DimWarehouse w 
                ON CAST(s.locationid AS SIGNED) = w.WarehouseID AND w.IsCurrent = 1
            WHERE p.ProductKey IS NULL 
               OR w.WarehouseKey IS NULL 
               OR CAST(s.quantity AS SIGNED) < 0;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: LOAD FACT TABLE ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks.FactInventory;")

            load_fact_sql = """
                INSERT INTO adventureworks.FactInventory (
                    InventoryDateKey, ProductKey, StoreKey, WarehouseKey, 
                    QuantityOnHand, StockAgingDays, ReorderLevel, SafetyStock
                )
                SELECT 
                    CAST(DATE_FORMAT(CURRENT_DATE(), '%Y%m%d') AS SIGNED), 
                    p.ProductKey,
                    0, 
                    w.WarehouseKey,
                    CAST(s.quantity AS INT),
                    DATEDIFF(CURRENT_DATE(), s.modifieddate),
                    p.ReorderPoint,
                    p.SafetyStockLevel
                FROM adventureworks_staging.stg_production_productinventory s
                INNER JOIN adventureworks.DimProduct p 
                    ON s.productid = p.productid AND p.IsCurrent = 1
                INNER JOIN adventureworks.DimWarehouse w 
                    ON CAST(s.locationid AS SIGNED) = w.WarehouseID AND w.IsCurrent = 1
                WHERE CAST(s.quantity AS SIGNED) >= 0;
            """
            starrocks_hook.run(load_fact_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="FactInventory",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during FactInventory transformation"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_load_inventory_data_into_factinventory_and_upload_to_starrocks()