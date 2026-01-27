import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

def on_task_failure(context):
    """
    Example of System Error Catching. 
    You can extend this to send Slack/Email alerts.
    """
    print(f"Task {context['task_instance'].task_id} failed!")

@dag(
    dag_id="update_aggregates_weekly_sales",
    schedule="0 3 * * 0", # Runs at 3:00 AM every Sunday
    start_date=pendulum.datetime(2014, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "aggregate", "weekly"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
        "on_failure_callback": on_task_failure
    }
)
def update_aggregates_weekly_sales():
    
    @task
    def compute_weekly_sales_aggregation():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        try:
            # 1. FIND THE LATEST MONDAY
            # We calculate the Monday for the most recent sales date
            date_query = """
                SELECT MAX(DATE_SUB(d.FullDate, INTERVAL (d.DayOfWeek - 1) DAY)) 
                FROM FactSales fs
                JOIN DimDate d ON fs.SalesDateKey = d.DateKey;
            """
            res = starrocks_hook.get_first(date_query)
            target_week = res[0]
            
            if not target_week:
                raise ValueError("No sales data found to aggregate.")

            print(f"Processing week starting: {target_week}")

            # 2. DELETE OLD DATA
            starrocks_hook.run(f"DELETE FROM agg_weekly_sales WHERE WeekStartDateKey = '{target_week}';")

            # 3. INSERT WEEKLY ROLLUP
            load_sql = f"""
                INSERT INTO agg_weekly_sales (
                    WeekStartDateKey, RegionKey, ProductCategoryKey, 
                    SumRevenue, AvgRevenue, MinRevenue, MaxRevenue
                )
                SELECT 
                    DATE_SUB(d.FullDate, INTERVAL (d.DayOfWeek - 1) DAY) as WeekStart,
                    fs.StoreKey,
                    dpc.ProductCategoryKey,
                    SUM(fs.SalesRevenue),
                    AVG(fs.SalesRevenue),
                    MIN(fs.SalesRevenue),
                    MAX(fs.SalesRevenue)
                FROM FactSales fs
                JOIN DimDate d ON fs.SalesDateKey = d.DateKey
                JOIN DimProduct dp ON fs.ProductKey = dp.ProductKey
                JOIN DimProductCategory dpc ON TRIM(dp.Category) = TRIM(dpc.CategoryName)
                WHERE DATE_SUB(d.FullDate, INTERVAL (d.DayOfWeek - 1) DAY) = '{target_week}'
                GROUP BY 1, 2, 3;
            """
            starrocks_hook.run(load_sql)

        except Exception as e:
            # Capture error in your new error table
            error_msg = str(e).replace("'", '"')
            starrocks_hook.run(f"""
                INSERT INTO adventureworks_errors.agg_weekly_sales_errors 
                (WeekStartDateKey, FailureReason, FailedAt, SQLState) 
                VALUES (CURRENT_DATE(), 'Logic Error: {error_msg[:200]}', NOW(), '1064')
            """)
            raise

    compute_weekly_sales_aggregation()

update_aggregates_weekly_sales()