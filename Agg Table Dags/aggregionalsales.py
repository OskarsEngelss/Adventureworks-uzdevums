import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="update_regional_sales_aggregation",
    schedule="0 5 1 * *", 
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "aggregate", "regional"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=10)
    }
)
def update_regional_sales_aggregation():
    
    @task
    def compute_regional_sales():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        current_target = None 

        try:
            # 1. FIND THE LATEST MONTH
            date_query = "SELECT DATE_TRUNC('month', MAX(SalesDateKey)) FROM FactSales;"
            res = starrocks_hook.get_first(date_query)
            target_month = res[0]
            current_target = target_month
            
            if not target_month:
                return

            # 2. CLEAN UP OLD DATA
            starrocks_hook.run(f"DELETE FROM agg_regional_sales WHERE MonthStartDateKey = '{target_month}';")

            # 3. INSERT ROLLUP WITH GROWTH RATE
            # We calculate GrowthRate by comparing current sum vs. the previous month's revenue in the same table
            load_sql = f"""
                INSERT INTO agg_regional_sales (
                    MonthStartDateKey,
                    RegionKey,
                    SalesTerritoryKey,
                    TotalRevenue,
                    GrowthRate
                )
                WITH MonthlyTotals AS (
                    SELECT 
                        CAST(DATE_TRUNC('month', fs.SalesDateKey) AS DATE) as MonthKey,
                        COALESCE(dr.RegionKey, 0) as RegKey,
                        COALESCE(dst.TerritoryKey, 0) as TerrKey,
                        SUM(fs.SalesRevenue) as Revenue
                    FROM FactSales fs
                    INNER JOIN DimStore ds ON fs.StoreKey = ds.StoreKey
                    LEFT JOIN DimRegion dr ON ds.Territory = dr.RegionName
                    LEFT JOIN DimSalesTerritory dst ON ds.Territory = dst.TerritoryName
                    WHERE fs.SalesDateKey >= '2011-01-01' AND fs.SalesDateKey <= '2024-12-31'
                    GROUP BY 1, 2, 3
                )
                SELECT 
                    mt.MonthKey,
                    mt.RegKey,
                    mt.TerrKey,
                    mt.Revenue,
                    -- THE UNCAP: Removing LEAST/GREATEST to allow true values
                    COALESCE(
                        ((mt.Revenue - LAG(mt.Revenue) OVER (PARTITION BY mt.RegKey, mt.TerrKey ORDER BY mt.MonthKey)) 
                        / NULLIF(LAG(mt.Revenue) OVER (PARTITION BY mt.RegKey, mt.TerrKey ORDER BY mt.MonthKey), 0)) * 100, 
                    0) as GrowthRate
                FROM MonthlyTotals mt
                WHERE mt.MonthKey IS NOT NULL;
            """
            starrocks_hook.run(load_sql)
            print(f"Regional aggregation for {target_month} complete.")
            
        except Exception as e:
            error_msg = str(e).replace("'", '"')
            error_date = current_target if current_target else datetime.date.today()
            
            starrocks_hook.run(f"""
                INSERT INTO adventureworks_errors.agg_regional_sales_errors 
                (MonthStartDateKey, FailureReason, FailedAt) 
                VALUES ('{error_date}', 'Regional Aggregate Failure: {error_msg[:200]}', NOW())
            """)
            raise

    compute_regional_sales()

update_regional_sales_aggregation()