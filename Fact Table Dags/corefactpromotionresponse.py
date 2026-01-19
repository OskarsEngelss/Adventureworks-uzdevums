import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Connection ID for StarRocks
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

        # 1. QUARANTINE: Identify and quarantine bad records
        quarantine_sql = """
            INSERT INTO adventureworks_errors.fact_promotionresponse_errors (
                SourceSalesOrderID, SourceSalesOrderDetailID, FailureReason, IsRecoverable, FailedData
            )
            SELECT 
                h.salesorderid,
                d.salesorderdetailid,
                CASE 
                    WHEN p.PromotionKey IS NULL THEN 'Invalid PromotionKey (SpecialOfferID)'
                    WHEN prod.ProductKey IS NULL THEN 'Missing ProductKey'
                END as FailureReason,
                1 as IsRecoverable,
                json_object(
                    'special_offer_id', CAST(d.specialofferid AS VARCHAR),
                    'product_id', CAST(d.productid AS VARCHAR),
                    'order_date', CAST(h.orderdate AS VARCHAR)
                ) as FailedData
            FROM adventureworks_staging.stg_sales_salesorderdetail d
            JOIN adventureworks_staging.stg_sales_salesorderheader h 
                ON d.salesorderid = h.salesorderid
            LEFT JOIN DimPromotion p 
                ON d.specialofferid = p.PromotionID
            LEFT JOIN DimProduct prod 
                ON d.productid = prod.ProductID
            WHERE d.specialofferid > 1 
              AND (p.PromotionKey IS NULL OR prod.ProductKey IS NULL);
        """
        
        # 2. LOAD: Main load into FactPromotionResponse
        load_sql = """
            INSERT INTO FactPromotionResponse (
                PromotionDateKey, ProductKey, StoreKey, PromotionKey, 
                SalesDuringCampaign, DiscountUsageCount, CustomerUptakeRate, PromotionROI
            )
            WITH CustBase AS (
                SELECT COUNT(*) as total_cust FROM DimCustomer WHERE IsCurrent = 1
            )
            SELECT 
                CAST(h.orderdate AS DATE) as PromotionDateKey,
                prod.ProductKey,
                COALESCE(ds.StoreKey, -1) as StoreKey, 
                promo.PromotionKey,
                -- LineTotal calculation: (UnitPrice * (1 - Discount)) * Qty
                SUM(d.unitprice * (1 - d.unitpricediscount) * CAST(d.orderqty AS DECIMAL)) as SalesDuringCampaign,
                COUNT(d.salesorderdetailid) as DiscountUsageCount,
                -- Rate Calculation
                (COUNT(DISTINCT h.customerid) / NULLIF((SELECT total_cust FROM CustBase LIMIT 1), 0)) * 100 as CustomerUptakeRate,
                -- ROI: Sales - (Cost * Qty)
                SUM(
                    (d.unitprice * (1 - d.unitpricediscount) * CAST(d.orderqty AS DECIMAL)) 
                    - (COALESCE(prod.Cost, 0) * CAST(d.orderqty AS DECIMAL))
                ) as PromotionROI
            FROM adventureworks_staging.stg_sales_salesorderdetail d
            INNER JOIN adventureworks_staging.stg_sales_salesorderheader h 
                ON d.salesorderid = h.salesorderid
            INNER JOIN DimPromotion promo 
                ON d.specialofferid = promo.PromotionID
            INNER JOIN DimProduct prod 
                ON d.productid = prod.ProductID
            -- THE DIMENSIONAL BRIDGE
            LEFT JOIN DimEmployee de 
                ON h.salespersonid = de.EmployeeID 
                AND de.IsCurrent = 1
            LEFT JOIN DimStore ds 
                ON de.StoreID = ds.StoreID 
                AND ds.IsCurrent = 1
            WHERE d.specialofferid > 1
            GROUP BY 1, 2, 3, 4;
        """

        starrocks_hook.run(quarantine_sql)
        starrocks_hook.run("TRUNCATE TABLE FactPromotionResponse;")
        starrocks_hook.run(load_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_load_promotion_data_into_factpromotionresponse_and_upload_to_starrocks()