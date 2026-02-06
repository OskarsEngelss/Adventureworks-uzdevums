import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="maintenance_truncate_all_adventureworks_tables",
    schedule=None,  # Manual run only
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "maintenance", "cleanup"],
    description="Wipes all data from the AdventureWorks warehouse to allow for a clean reload"
)
def truncate_warehouse():

    @task
    def execute_truncation():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        # The list provided from your SHOW TABLES command
        tables = [
            "DimAgingTier", "DimCustomer", "DimCustomerSegment", "DimDate",
            "DimEmployee", "DimFeedbackCategory", "DimFinanceCategory",
            "DimProduct", "DimProductCategory", "DimPromotion", "DimRegion",
            "DimReturnReason", "DimSalesTerritory", "DimStore", "DimVendor",
            "DimWarehouse", "FactCustomerFeedback", "FactEmployeeSales",
            "FactFinance", "FactInventory", "FactProduction", "FactPromotionResponse",
            "FactPurchases", "FactReturns", "FactSales", "agg_daily_inventory",
            "agg_daily_sales", "agg_monthly_product_performance", 
            "agg_monthly_sales", "agg_regional_sales", "agg_weekly_sales"
        ]
        
        print(f"Starting truncation of {len(tables)} tables...")

        for table in tables:
            try:
                # Using the adventureworks database context
                truncate_sql = f"TRUNCATE TABLE adventureworks.{table};"
                starrocks_hook.run(truncate_sql)
                print(f"✅ Successfully truncated: {table}")
            except Exception as e:
                print(f"❌ Failed to truncate {table}: {str(e)}")
                # We continue to the next table even if one fails
                continue

        print("Finished cleanup process.")

    execute_truncation()

truncate_warehouse_dag = truncate_warehouse()