import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

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

        # 1. QUARANTINE: Catching missing Vendors, Products, or bad costs
        quarantine_sql = """
            INSERT INTO adventureworks_errors.fact_purchases_errors (
                PurchaseOrderID, PurchaseOrderDetailID, FailureReason, IsRecoverable, FailedData
            )
            SELECT 
                d.purchaseorderid, 
                d.purchaseorderdetailid,
                CASE 
                    WHEN h.purchaseorderid IS NULL THEN 'Missing Header'
                    WHEN v.VendorKey IS NULL THEN 'Missing VendorKey'
                    WHEN p.ProductKey IS NULL THEN 'Missing ProductKey'
                    WHEN d.unitprice < 0 THEN 'Negative Unit Price'
                END as FailureReason,
                1 as IsRecoverable,
                json_object(
                    'po_id', CAST(d.purchaseorderid AS VARCHAR),
                    'line_id', CAST(d.purchaseorderdetailid AS VARCHAR),
                    'vendor_id', CAST(h.vendorid AS VARCHAR),
                    'product_id', CAST(d.productid AS VARCHAR)
                ) as FailedData
            FROM adventureworks_staging.stg_purchasing_purchaseorderdetail d
            LEFT JOIN adventureworks_staging.stg_purchasing_purchaseorderheader h 
                ON d.purchaseorderid = h.purchaseorderid
            LEFT JOIN DimVendor v ON h.vendorid = v.VendorID AND v.IsCurrent = TRUE
            LEFT JOIN DimProduct p ON d.productid = p.productid AND p.IsCurrent = TRUE
            WHERE h.purchaseorderid IS NULL 
               OR v.VendorKey IS NULL 
               OR p.ProductKey IS NULL 
               OR d.unitprice < 0;
        """
        starrocks_hook.run(quarantine_sql)

        # 2. LOAD FACT TABLE: Grain is one row per PO Line Item
        load_fact_sql = """
            INSERT INTO FactPurchases (
                PurchaseDateKey, ProductKey, VendorKey, 
                PurchaseAmount, PurchaseQuantity, DiscountAmount, UnitCost
            )
            SELECT 
                CAST(h.orderdate AS DATE),
                p.ProductKey,
                v.VendorKey,
                (d.unitprice * CAST(d.orderqty AS DECIMAL)),
                CAST(d.orderqty AS INT),
                0.00,
                d.unitprice
            FROM adventureworks_staging.stg_purchasing_purchaseorderdetail d
            INNER JOIN adventureworks_staging.stg_purchasing_purchaseorderheader h 
                ON d.purchaseorderid = h.purchaseorderid
            INNER JOIN DimProduct p ON d.productid = p.productid AND p.IsCurrent = TRUE
            INNER JOIN DimVendor v ON h.vendorid = v.VendorID AND v.IsCurrent = TRUE
            WHERE d.unitprice >= 0;
        """
        starrocks_hook.run(load_fact_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_load_purchases_data_into_factpurchases_and_upload_to_starrocks()