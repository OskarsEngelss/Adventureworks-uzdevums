import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_load_promotion_data_into_factpromotionresponse_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "fact", "promotion", "adventureworks"],
    default_args={"retries": 1, "retry_delay": datetime.timedelta(minutes=5)}
)
def extract_transform_load_promotion_data_into_factpromotionresponse_and_upload_to_starrocks():

    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()

        # 1. QUARANTINE
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'FactPromotionResponse',
                CONCAT(CAST(h.salesorderid AS CHAR), '-', CAST(d.salesorderdetailid AS CHAR)),
                CASE 
                    WHEN p.PromotionKey IS NULL THEN 'Invalid Promotion (SpecialOfferID)'
                    WHEN prod.ProductKey IS NULL THEN 'Missing ProductKey'
                END,
                1, 0, 0,
                json_object(
                    'special_offer_id', CAST(d.specialofferid AS CHAR),
                    'product_id', CAST(d.productid AS CHAR),
                    'order_date', CAST(h.orderdate AS CHAR)
                )
            FROM adventureworks_staging.stg_sales_salesorderdetail d
            JOIN adventureworks_staging.stg_sales_salesorderheader h ON d.salesorderid = h.salesorderid
            LEFT JOIN adventureworks.DimPromotion p ON d.specialofferid = p.PromotionID
            LEFT JOIN adventureworks.DimProduct prod ON d.productid = prod.ProductID
            WHERE d.specialofferid > 1 AND (p.PromotionKey IS NULL OR prod.ProductKey IS NULL);
        """
        starrocks_hook.run(quarantine_sql)
        
        try:
            # 2. LOAD
            load_sql = """
                INSERT INTO adventureworks.FactPromotionResponse (
                    PromotionDateKey, ProductKey, StoreKey, PromotionKey, 
                    SalesDuringCampaign, DiscountUsageCount, CustomerUptakeRate, PromotionROI
                )
                WITH CustBase AS (
                    SELECT COUNT(*) as total_cust FROM adventureworks.DimCustomer WHERE IsCurrent = 1
                )
                SELECT 
                    CAST(DATE_FORMAT(h.orderdate, '%Y%m%d') AS SIGNED) as PromotionDateKey,
                    prod.ProductKey,
                    COALESCE(ds.StoreKey, 0) as StoreKey, 
                    promo.PromotionKey,
                    SUM(d.unitprice * (1 - d.unitpricediscount) * CAST(d.orderqty AS DECIMAL)) as SalesDuringCampaign,
                    COUNT(d.salesorderdetailid) as DiscountUsageCount,
                    (COUNT(DISTINCT h.customerid) / NULLIF((SELECT total_cust FROM CustBase), 0)) * 100 as CustomerUptakeRate,
                    SUM(
                        (d.unitprice * (1 - d.unitpricediscount) * CAST(d.orderqty AS DECIMAL)) 
                        - (COALESCE(prod.Cost, 0) * CAST(d.orderqty AS DECIMAL))
                    ) as PromotionROI
                FROM adventureworks_staging.stg_sales_salesorderdetail d
                INNER JOIN adventureworks_staging.stg_sales_salesorderheader h 
                    ON d.salesorderid = h.salesorderid
                INNER JOIN adventureworks.DimPromotion promo 
                    ON d.specialofferid = promo.PromotionID
                INNER JOIN adventureworks.DimProduct prod 
                    ON d.productid = prod.ProductID
                LEFT JOIN adventureworks.DimEmployee de 
                    ON h.salespersonid = de.EmployeeID 
                    AND de.IsCurrent = 1
                LEFT JOIN adventureworks.DimStore ds 
                    ON de.StoreID = ds.StoreID 
                    AND ds.IsCurrent = 1
                WHERE d.specialofferid > 1
                GROUP BY 1, 2, 3, 4;
            """

            starrocks_hook.run("TRUNCATE TABLE adventureworks.FactPromotionResponse;")
            starrocks_hook.run(load_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="FactPromotionResponse",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during FactPromotionResponse transformation"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_load_promotion_data_into_factpromotionresponse_and_upload_to_starrocks()