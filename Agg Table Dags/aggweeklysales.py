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
            # 1. FIND THE LATEST MONDAY (Aprēķinām pirmdienu kā BIGINT YYYYMMDD)
            date_query = """
                SELECT MAX(
                    CAST(DATE_FORMAT(
                        DATE_SUB(STR_TO_DATE(CAST(SalesDateKey AS CHAR), '%Y%m%d'), 
                        INTERVAL (DAYOFWEEK(STR_TO_DATE(CAST(SalesDateKey AS CHAR), '%Y%m%d')) - 1) DAY), 
                    '%Y%m%d') AS BIGINT)
                ) 
                FROM FactSales;
            """
            res = starrocks_hook.get_first(date_query)
            target_week = res[0]
            
            if not target_week:
                raise ValueError("No sales data found to aggregate.")

            print(f"Processing week starting: {target_week}")

            # 2. DELETE OLD DATA (Pēdiņas noņemtas)
            starrocks_hook.run(f"DELETE FROM agg_weekly_sales WHERE WeekStartDateKey = {target_week};")

            # 3. INSERT WEEKLY ROLLUP
            load_sql = f"""
                INSERT INTO agg_weekly_sales (
                    WeekStartDateKey, RegionKey, ProductCategoryKey, 
                    SumRevenue, AvgRevenue, MinRevenue, MaxRevenue
                )
                SELECT 
                    CAST(DATE_FORMAT(
                        DATE_SUB(STR_TO_DATE(CAST(fs.SalesDateKey AS CHAR), '%Y%m%d'), 
                        INTERVAL (DAYOFWEEK(STR_TO_DATE(CAST(fs.SalesDateKey AS CHAR), '%Y%m%d')) - 1) DAY), 
                    '%Y%m%d') AS BIGINT) as WeekStart,
                    fs.StoreKey,
                    dpc.ProductCategoryKey,
                    SUM(fs.SalesRevenue),
                    AVG(fs.SalesRevenue),
                    MIN(fs.SalesRevenue),
                    MAX(fs.SalesRevenue)
                FROM FactSales fs
                JOIN DimProduct dp ON fs.ProductKey = dp.ProductKey
                JOIN DimProductCategory dpc ON TRIM(dp.Category) = TRIM(dpc.CategoryName)
                WHERE CAST(DATE_FORMAT(
                        DATE_SUB(STR_TO_DATE(CAST(fs.SalesDateKey AS CHAR), '%Y%m%d'), 
                        INTERVAL (DAYOFWEEK(STR_TO_DATE(CAST(fs.SalesDateKey AS CHAR), '%Y%m%d')) - 1) DAY), 
                    '%Y%m%d') AS BIGINT) = {target_week}
                GROUP BY 1, 2, 3;
            """
            starrocks_hook.run(load_sql)

        except Exception as e:
            # Capture error in your new error table
            error_msg = str(e).replace("'", '"')
            # Nodrošinām skaitlisku vērtību kļūdu tabulai
            err_week = target_week if 'target_week' in locals() and target_week else 19000101
            starrocks_hook.run(f"""
                INSERT INTO adventureworks_errors.agg_weekly_sales_errors 
                (WeekStartDateKey, FailureReason, FailedAt, SQLState) 
                VALUES ({err_week}, 'Logic Error: {error_msg[:200]}', NOW(), '1064')
            """)
            raise

    compute_weekly_sales_aggregation()

update_aggregates_weekly_sales()