import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="update_aggregates_daily_sales",
    schedule="0 2 * * *", # Runs at 2:00 AM daily
    start_date=pendulum.datetime(2014, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "aggregate", "sales"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=10),
    }
)
def update_aggregates_daily_sales():
    
    @task
    def compute_daily_sales_aggregation():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        try:
            # 1. SMART DATE LOOKUP
            df = starrocks_hook.get_first("SELECT MAX(SalesDateKey) FROM FactSales;")
            target_date = df[0] if df[0] else '2026-01-19'
            
            print(f"Detected latest sales data on: {target_date}. Processing aggregates...")

            # 2. DELETE (Idempotency)
            starrocks_hook.run(f"DELETE FROM agg_daily_sales WHERE SalesDateKey = '{target_date}';")

            # 3. INSERT
            quarantine_sql = f"""
                INSERT INTO agg_daily_sales (
                    SalesDateKey, StoreKey, ProductCategoryKey, 
                    TotalRevenue, TotalQuantity, TotalDiscount, TransactionCount
                )
                SELECT 
                    fs.SalesDateKey,
                    fs.StoreKey,
                    dpc.ProductCategoryKey,
                    SUM(fs.SalesRevenue),
                    SUM(fs.QuantitySold),
                    SUM(fs.DiscountAmount),
                    COUNT(*)
                FROM FactSales fs
                INNER JOIN DimProduct dp ON fs.ProductKey = dp.ProductKey AND dp.IsCurrent = TRUE
                INNER JOIN DimProductCategory dpc ON dp.Category = dpc.CategoryName
                WHERE fs.SalesDateKey = '{target_date}'
                GROUP BY fs.SalesDateKey, fs.StoreKey, dpc.ProductCategoryKey;
            """
            starrocks_hook.run(quarantine_sql)
            
        except Exception as e:
            error_msg = str(e).replace("'", '"')
            starrocks_hook.run(f"""
                INSERT INTO adventureworks_errors.agg_daily_sales_errors 
                (SalesDateKey, FailureReason, FailedAt) 
                VALUES ('{target_date}', 'Daily Sales Agg Failure: {error_msg[:200]}', NOW())
            """)
            raise
        
    compute_daily_sales_aggregation()

update_aggregates_daily_sales()