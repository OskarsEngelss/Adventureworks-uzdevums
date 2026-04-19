import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="update_agg_monthly_product_performance",
    schedule="0 6 1 * *",
    start_date=pendulum.datetime(2011, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "aggregate", "product", "returns"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=10),
    }
)
def update_agg_monthly_product_performance():
    
    @task
    def compute_monthly_product_performance():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        try:
            # 1. FIND LATEST MONTH FROM SALES (Pārveidots uz BIGINT mēneša sākumu)
            date_query = "SELECT MAX(SalesDateKey / 100 * 100 + 1) FROM FactSales;"
            res = starrocks_hook.get_first(date_query)
            target_month = res[0]
            
            if not target_month:
                print("No data found in FactSales. Skipping.")
                return

            print(f"Aggregating Product Performance for: {target_month}")

            # 2. IDEMPOTENT DELETE (Pēdiņas noņemtas)
            starrocks_hook.run(f"DELETE FROM agg_monthly_product_performance WHERE MonthStartDateKey = {target_month};")

            # 3. INSERT MULTI-FACT AGGREGATION
            load_sql = f"""
                INSERT INTO agg_monthly_product_performance (
                    MonthStartDateKey, ProductKey, StoreKey, 
                    TotalRevenue, UnitsSold, ReturnRate, AvgRating
                )
                WITH MonthlySales AS (
                    SELECT 
                        (SalesDateKey / 100 * 100 + 1) as MonthKey,
                        ProductKey,
                        StoreKey,
                        CustomerKey,
                        SUM(SalesRevenue) as Revenue,
                        SUM(QuantitySold) as SoldQty
                    FROM FactSales
                    WHERE (SalesDateKey / 100 * 100 + 1) = {target_month}
                    GROUP BY 1, 2, 3, 4
                ),
                MonthlyReturns AS (
                    SELECT 
                        (ReturnDateKey / 100 * 100 + 1) as MonthKey,
                        ProductKey,
                        StoreKey,
                        SUM(ReturnedQuantity) as ReturnedQty
                    FROM FactReturns
                    WHERE (ReturnDateKey / 100 * 100 + 1) = {target_month}
                    GROUP BY 1, 2, 3
                ),
                MonthlyFeedback AS (
                    SELECT 
                        (f.FeedbackDateKey / 100 * 100 + 1) as MonthKey,
                        s.ProductKey,
                        AVG(f.FeedbackScore) as RatingScore
                    FROM FactCustomerFeedback f
                    JOIN FactSales s ON f.CustomerKey = s.CustomerKey 
                        AND (f.FeedbackDateKey / 100 * 100 + 1) = (s.SalesDateKey / 100 * 100 + 1)
                    WHERE (f.FeedbackDateKey / 100 * 100 + 1) = {target_month}
                    GROUP BY 1, 2
                ),
                FinalAgg AS (
                    SELECT 
                        ms.MonthKey,
                        ms.ProductKey,
                        ms.StoreKey,
                        SUM(ms.Revenue) as TotalRevenue,
                        SUM(ms.SoldQty) as TotalUnits,
                        COALESCE(MAX(mr.ReturnedQty), 0) as ReturnedQty,
                        COALESCE(MAX(mf.RatingScore), 0) as Rating
                    FROM MonthlySales ms
                    LEFT JOIN MonthlyReturns mr 
                        ON ms.MonthKey = mr.MonthKey 
                        AND ms.ProductKey = mr.ProductKey 
                        AND ms.StoreKey = mr.StoreKey
                    LEFT JOIN MonthlyFeedback mf 
                        ON ms.MonthKey = mf.MonthKey 
                        AND ms.ProductKey = mf.ProductKey
                    GROUP BY 1, 2, 3
                )
                SELECT 
                    MonthKey,
                    ProductKey,
                    StoreKey,
                    TotalRevenue,
                    TotalUnits,
                    CAST((ReturnedQty * 100.0) / NULLIF(TotalUnits, 0) AS DECIMAL(5,2)),
                    CAST(Rating AS DECIMAL(5,2))
                FROM FinalAgg;
            """
            starrocks_hook.run(load_sql)
            print("Product performance aggregation complete.")

        except Exception as e:
            error_msg = str(e).replace("'", '"')
            # Ja target_month nav, lietojam skaitlisku noklusējuma vērtību
            err_month = target_month if 'target_month' in locals() and target_month else 19000101
            starrocks_hook.run(f"""
                INSERT INTO adventureworks_errors.agg_monthly_product_performance_errors 
                (MonthStartDateKey, FailureReason, FailedAt) 
                VALUES ({err_month}, 
                        'Product Agg Failure: {error_msg[:200]}', NOW())
            """)
            raise

    compute_monthly_product_performance()

update_agg_monthly_product_performance()