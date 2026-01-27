import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_purchases_data_into_factpurchases_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "purchasing", "adventureworks"],
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    }
)
def extract_transform_load_purchases_data_into_factpurchases_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()

        # --- STEP 1: QUARANTINE ---
        # Identifying missing dimensions or data quality issues (Negative Prices)
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'FactPurchases',
                CONCAT(CAST(d.purchaseorderid AS CHAR), '-', CAST(d.purchaseorderdetailid AS CHAR)),
                CASE 
                    WHEN h.purchaseorderid IS NULL THEN 'Missing Header'
                    WHEN v.VendorKey IS NULL THEN 'Missing VendorKey'
                    WHEN p.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN d.unitprice < 0 THEN 'Negative Unit Price'
                END,
                1, 0, 0,
                json_object(
                    'po_id', CAST(d.purchaseorderid AS CHAR),
                    'line_id', CAST(d.purchaseorderdetailid AS CHAR),
                    'vendor_id', CAST(h.vendorid AS CHAR),
                    'product_id', CAST(d.productid AS CHAR)
                )
            FROM adventureworks_staging.stg_purchasing_purchaseorderdetail d
            LEFT JOIN adventureworks_staging.stg_purchasing_purchaseorderheader h 
                ON d.purchaseorderid = h.purchaseorderid
            LEFT JOIN adventureworks.DimVendor v 
                ON h.vendorid = v.VendorID AND v.IsCurrent = 1
            LEFT JOIN adventureworks.DimProduct p 
                ON d.productid = p.productid AND p.IsCurrent = 1
            WHERE h.purchaseorderid IS NULL 
               OR v.VendorKey IS NULL 
               OR p.ProductKey IS NULL 
               OR d.unitprice < 0;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: LOAD FACT TABLE ---
            # Truncate and reload pattern for small-to-medium datasets
            starrocks_hook.run("TRUNCATE TABLE adventureworks.FactPurchases;")

            load_fact_sql = """
                INSERT INTO adventureworks.FactPurchases (
                    PurchaseDateKey, ProductKey, VendorKey, 
                    PurchaseAmount, PurchaseQuantity, DiscountAmount, UnitCost
                )
                SELECT 
                    CAST(h.orderdate AS DATE),
                    p.ProductKey,
                    v.VendorKey,
                    (d.unitprice * CAST(d.orderqty AS DECIMAL)),
                    CAST(d.orderqty AS INT),
                    0.00, -- Placeholder if discounts are added to schema later
                    d.unitprice
                FROM adventureworks_staging.stg_purchasing_purchaseorderdetail d
                INNER JOIN adventureworks_staging.stg_purchasing_purchaseorderheader h 
                    ON d.purchaseorderid = h.purchaseorderid
                INNER JOIN adventureworks.DimProduct p 
                    ON d.productid = p.productid AND p.IsCurrent = 1
                INNER JOIN adventureworks.DimVendor v 
                    ON h.vendorid = v.VendorID AND v.IsCurrent = 1
                WHERE d.unitprice >= 0;
            """
            starrocks_hook.run(load_fact_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="FactPurchases",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during FactPurchases transformation"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_load_purchases_data_into_factpurchases_and_upload_to_starrocks()