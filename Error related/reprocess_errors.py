import datetime
import json
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.email import EmailOperator

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="reprocess_recoverable_errors",
    schedule="0 6 * * *", 
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["maintenance", "recovery", "task-7"],
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=60),
        "email": ["data-warehouse@company.com"],
        "email_on_failure": True 
    }
)
def reprocess_recoverable_errors():

    @task
    def heal_row_level_errors():
        """
        FULFILLS TASK 7: Dims and Facts recovery.
        Looks for missing keys in staging and pushes them to final tables.
        """
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        # 1. Get the failed records that are recoverable
        error_query = """
            SELECT ErrorID, SourceTable, RecordNaturalKey, RetryCount 
            FROM adventureworks_errors.error_records
            WHERE IsResolved = 0 AND IsRecoverable = 1 AND RetryCount < 3 
              AND ErrorDate >= now() - INTERVAL 1 DAY
            LIMIT 100;
        """
        errors = starrocks_hook.get_records(error_query)
        critical_manual_review = []
        
        # 2. Configuration for staging lookups and final inserts
        # Format: 'TableName': (StagingTable, KeyColumn, FinalTable)
        recovery_config = {
            'DimProduct': ('adventureworks_staging.stg_production_product', 'productid', 'adventureworks_dw.DimProduct'),
            'DimVendor': ('adventureworks_staging.stg_purchasing_vendor', 'businessentityid', 'adventureworks_dw.DimVendor'),
            'DimCustomer': ('adventureworks_staging.stg_sales_customer', 'customerid', 'adventureworks_dw.DimCustomer'),
            'FactSales': ('adventureworks_staging.stg_sales_salesorderdetail', 'salesorderdetailid', 'adventureworks_dw.FactSales'),
            'FactInventory': ('adventureworks_staging.stg_production_productinventory', 'productid', 'adventureworks_dw.FactInventory')
        }

        for error_id, table, nat_key, retries in errors:
            healed = False
            if table in recovery_config:
                stg_table, stg_col, final_table = recovery_config[table]
                
                # Check if the data is now in staging
                check_res = starrocks_hook.get_first(f"SELECT 1 FROM {stg_table} WHERE {stg_col} = '{nat_key}'")
                
                if check_res:
                    try:
                        # HEALING ACTION: Move from staging to final
                        # Note: This assumes column names match. 
                        starrocks_hook.run(f"INSERT INTO {final_table} SELECT * FROM {stg_table} WHERE {stg_col} = '{nat_key}'")
                        
                        # Mark as resolved
                        starrocks_hook.run(f"""
                            UPDATE adventureworks_errors.error_records 
                            SET IsResolved = 1, ResolutionComment = 'Healed: Row inserted from staging', LastAttemptDate = NOW()
                            WHERE ErrorID = {error_id}
                        """)
                        healed = True
                    except Exception as e:
                        print(f"Heal insert failed for {table}: {e}")

            if not healed:
                # Increment retry count
                new_count = retries + 1
                starrocks_hook.run(f"UPDATE adventureworks_errors.error_records SET RetryCount = {new_count}, LastAttemptDate = NOW() WHERE ErrorID = {error_id}")
                if new_count >= 3:
                    critical_manual_review.append(f"Table: {table} | Key: {nat_key}")

        return critical_manual_review

    @task
    def heal_aggregate_errors():
        """
        FULFILLS TASK 7: Aggregate recovery.
        Re-calculates Sales summaries for dates that failed.
        """
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        aggregates = [
            {
                'name': 'Daily Sales',
                'err_table': 'adventureworks_errors.agg_daily_sales_errors',
                'target': 'agg_daily_sales',
                'date_col': 'SalesDateKey',
                'sql': """
                    INSERT INTO agg_daily_sales (SalesDateKey, StoreKey, ProductCategoryKey, TotalRevenue, TotalQuantity)
                    SELECT fs.SalesDateKey, fs.StoreKey, dp.ProductCategoryKey, SUM(fs.SalesRevenue), SUM(fs.SalesQuantity)
                    FROM adventureworks_dw.FactSales fs
                    JOIN adventureworks_dw.DimProduct dp ON fs.ProductKey = dp.ProductKey
                    WHERE fs.SalesDateKey = '{date_val}'
                    GROUP BY 1, 2, 3;
                """
            },
            {
                'name': 'Regional Sales',
                'err_table': 'adventureworks_errors.agg_regional_sales_errors',
                'target': 'agg_regional_sales',
                'date_col': 'MonthStartDateKey',
                'sql': """
                    INSERT INTO agg_regional_sales (MonthStartDateKey, RegionKey, TotalRevenue)
                    SELECT MonthStartDateKey, RegionKey, SUM(SalesRevenue)
                    FROM adventureworks_dw.FactSales
                    WHERE MonthStartDateKey = '{date_val}'
                    GROUP BY 1, 2;
                """
            }
        ]

        for agg in aggregates:
            failed = starrocks_hook.get_records(f"SELECT ErrorID, {agg['date_col']} FROM {agg['err_table']} WHERE IsResolved = 0")
            for err_id, date_val in failed:
                try:
                    # Clean up partial/bad data first
                    starrocks_hook.run(f"DELETE FROM {agg['target']} WHERE {agg['date_col']} = '{date_val}'")
                    # Re-run the logic
                    starrocks_hook.run(agg['sql'].format(date_val=date_val))
                    # Mark success
                    starrocks_hook.run(f"UPDATE {agg['err_table']} SET IsResolved = 1, LastAttemptDate = NOW() WHERE ErrorID = {err_id}")
                except Exception as e:
                    print(f"Failed to heal aggregate {agg['name']}: {e}")

    @task
    def alert_team_on_max_retries(failed_list):
        if failed_list:
            body = f"<h3>Manual Review Required</h3><p>Records hit max retries:</p><ul>"
            for item in failed_list:
                body += f"<li>{item}</li>"
            body += "</ul>"
            
            EmailOperator(
                task_id="send_manual_review_email",
                to="data-warehouse@company.com",
                subject="ALERT: Reprocessing Task - Max Retries Reached",
                html_content=body
            ).execute(context=None)

    # Execution Flow
    critical_errors = heal_row_level_errors()
    heal_aggregate_errors()
    
    alert_team_on_max_retries(critical_errors)

reprocess_recoverable_errors()