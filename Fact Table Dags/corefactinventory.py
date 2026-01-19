import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

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

        # 1. QUARANTINE (Same as before)
        quarantine_sql = """
            INSERT INTO adventureworks_errors.fact_inventory_errors (
                ProductID, LocationID, FailureReason, IsRecoverable, FailedData
            )
            SELECT 
                s.productid, s.locationid,
                CASE 
                    WHEN p.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN s.quantity < 0 THEN 'Negative Quantity'
                END as FailureReason,
                1 as IsRecoverable,
                json_object(
                    'product_id', CAST(s.productid AS VARCHAR),
                    'location_id', CAST(s.locationid AS VARCHAR),
                    'quantity', CAST(s.quantity AS VARCHAR)
                ) as FailedData
            FROM adventureworks_staging.stg_production_productinventory s
            LEFT JOIN DimProduct p ON s.productid = p.productid AND p.IsCurrent = TRUE
            WHERE p.ProductKey IS NULL OR s.quantity < 0;
        """
        starrocks_hook.run(quarantine_sql)

        # 2. LOAD FACT TABLE: Now pulling real values from DimProduct
        load_fact_sql = """
            INSERT INTO FactInventory (
                InventoryDateKey, ProductKey, StoreKey, WarehouseKey, 
                QuantityOnHand, StockAgingDays, ReorderLevel, SafetyStock
            )
            SELECT 
                CURRENT_DATE(), 
                p.ProductKey,
                0, 
                s.locationid, 
                CAST(s.quantity AS INT),
                -- CALCULATION: Days since the stock record was last modified
                DATEDIFF(CURRENT_DATE(), s.modifieddate) as StockAgingDays,
                p.ReorderPoint,
                p.SafetyStockLevel
            FROM adventureworks_staging.stg_production_productinventory s
            INNER JOIN DimProduct p ON s.productid = p.productid AND p.IsCurrent = TRUE
            WHERE s.quantity >= 0;
        """
        starrocks_hook.run(load_fact_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_load_inventory_data_into_factinventory_and_upload_to_starrocks()