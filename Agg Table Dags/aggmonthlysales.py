import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="update_aggregates_monthly_sales",
    schedule="0 4 1 * *", 
    start_date=pendulum.datetime(2014, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "aggregate", "monthly"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=10)
    }
)
def update_aggregates_monthly_sales():
    
    @task
    def compute_monthly_aggregation():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        try:
            # 1. FIND THE LATEST MONTH (Izmantojam BIGINT matemātiku, lai iegūtu YYYYMM01)
            date_query = """
                SELECT MAX(SalesDateKey / 100 * 100 + 1)
                FROM FactSales;
            """
            res = starrocks_hook.get_first(date_query)
            target_month = res[0]
            
            if not target_month:
                print("No data found in FactSales.")
                return

            print(f"Aggregating data for month starting: {target_month}")

            # 2. CLEAN UP OLD DATA (Pēdiņas noņemtas)
            starrocks_hook.run(f"DELETE FROM agg_monthly_sales WHERE MonthStartDateKey = {target_month};")

            # 3. INSERT CLEAN ROLLUP
            load_sql = f"""
                INSERT INTO agg_monthly_sales (
                    MonthStartDateKey, CustomerSegmentKey, RegionKey, 
                    TotalRevenue, AvgOrderValue, DistinctCustomerCount
                )
                SELECT 
                    (fs.SalesDateKey / 100 * 100 + 1),
                    COALESCE(dcs.SegmentKey, 0),
                    fs.StoreKey,
                    SUM(fs.SalesRevenue),
                    AVG(fs.SalesRevenue),
                    COUNT(DISTINCT fs.CustomerKey)
                FROM FactSales fs
                LEFT JOIN (
                    -- This subquery ensures we only get 1 row per customer
                    SELECT DISTINCT CustomerKey, CustomerSegment 
                    FROM DimCustomer 
                    WHERE IsCurrent = TRUE
                ) dc ON fs.CustomerKey = dc.CustomerKey
                LEFT JOIN DimCustomerSegment dcs ON dc.CustomerSegment = dcs.SegmentName
                WHERE (fs.SalesDateKey / 100 * 100 + 1) = {target_month}
                GROUP BY 1, 2, 3;
            """
            starrocks_hook.run(load_sql)
            print("Monthly aggregation complete.")
            
        except Exception as e:
            # Log the error to your dedicated error table
            error_msg = str(e).replace("'", '"')
            # Ja target_month nav, izmantojam CURRENT_DATE formātā YYYYMMDD
            err_month = target_month if target_month else int(datetime.date.today().strftime('%Y%m01'))
            starrocks_hook.run(f"""
                INSERT INTO adventureworks_errors.agg_monthly_sales_errors 
                (MonthStartDateKey, FailureReason, FailedAt) 
                VALUES ({err_month}, 'Monthly Aggregate Failure: {error_msg[:200]}', NOW())
            """)
            raise

    compute_monthly_aggregation()

update_aggregates_monthly_sales()