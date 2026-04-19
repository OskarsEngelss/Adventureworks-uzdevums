import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="update_aggregates_daily_inventory",
    schedule="0 5 * * *", # Runs daily at 5:00 AM
    start_date=pendulum.datetime(2014, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "aggregate", "inventory"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=10)
    }
)
def update_aggregates_daily_inventory():
    
    @task
    def compute_daily_inventory_aggregation():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        # Initialize target_date to handle errors before the first query
        target_date = None
        
        try:
            # 1. FIND THE LATEST DATE IN FACT
            date_query = "SELECT MAX(InventoryDateKey) FROM FactInventory;"
            res = starrocks_hook.get_first(date_query)
            target_date = res[0]
            
            if not target_date:
                print("No data found in FactInventory.")
                return

            print(f"Aggregating inventory for date: {target_date}")

            # 2. IDEMPOTENT DELETE (Pēdiņas noņemtas, jo tips ir BIGINT)
            starrocks_hook.run(f"DELETE FROM agg_daily_inventory WHERE InventoryDateKey = {target_date};")

            # 3. INSERT ROLLUP
            load_sql = f"""
                INSERT INTO agg_daily_inventory (
                    InventoryDateKey, 
                    WarehouseKey, 
                    ProductCategoryKey, 
                    AgingTierKey, 
                    AvgInventoryValue
                )
                SELECT 
                    fi.InventoryDateKey,
                    fi.WarehouseKey,
                    COALESCE(dpc.ProductCategoryKey, 0),
                    COALESCE(dat.AgingTierKey, 0),
                    -- Average of (Quantity * Product Cost)
                    AVG(fi.QuantityOnHand * dp.Cost)
                FROM FactInventory fi
                INNER JOIN (
                    -- Get clean list of current products
                    SELECT ProductKey, Category, Cost 
                    FROM DimProduct 
                    WHERE IsCurrent = TRUE
                ) dp ON fi.ProductKey = dp.ProductKey
                LEFT JOIN DimProductCategory dpc ON TRIM(dp.Category) = TRIM(dpc.CategoryName)
                LEFT JOIN DimAgingTier dat ON fi.StockAgingDays >= dat.MinAgingDays 
                                          AND fi.StockAgingDays <= dat.MaxAgingDays
                WHERE fi.InventoryDateKey = {target_date} -- Pēdiņas noņemtas
                GROUP BY 1, 2, 3, 4;
            """
            starrocks_hook.run(load_sql)
            print("Daily inventory aggregation successful.")
            
        except Exception as e:
            # Log the error to your dedicated error table
            error_msg = str(e).replace("'", '"')
            # Ja target_date nav atrasts, izmantojam šodienu formātā YYYYMMDD
            log_date = target_date if target_date else int(datetime.date.today().strftime('%Y%m%d'))
            
            starrocks_hook.run(f"""
                INSERT INTO adventureworks_errors.agg_daily_inventory_errors 
                (InventoryDateKey, FailureReason, FailedAt) 
                VALUES ({log_date}, 'Inventory Agg Failure: {error_msg[:200]}', NOW())
            """)
            raise

    compute_daily_inventory_aggregation()

update_aggregates_daily_inventory()