import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_product_data_into_dimproduct_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimproduct", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_product_data_into_dimproduct_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()
        yesterday = pendulum.now().subtract(days=1).to_date_string()

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_product_upsert WHERE ProductID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING (Including Inventory attributes)
        load_staging_sql = f"""
            INSERT INTO adventureworks_staging.stg_dim_product_upsert
            SELECT 
                p.productid,
                p.name AS ProductName,
                p.productnumber AS SKU,
                pc.CategoryName AS Category,
                ps.name AS SubCategory,
                pm.name AS Brand,
                p.listprice,
                p.standardcost AS Cost,
                CASE 
                    WHEN p.sellenddate IS NOT NULL AND p.sellenddate <= CURRENT_DATE() THEN 'Discontinued'
                    WHEN p.sellstartdate > CURRENT_DATE() THEN 'Coming Soon'
                    ELSE 'Active'
                END AS ProductStatus,
                p.color,
                p.size,
                p.weight,
                p.reorderpoint,       -- New source column
                p.safetystocklevel,   -- New source column
                CURRENT_DATE() as SourceUpdateDate
            FROM adventureworks_staging.stg_production_product p
            LEFT JOIN adventureworks_staging.stg_production_productsubcategory ps 
                ON p.productsubcategoryid = ps.productsubcategoryid
            LEFT JOIN DimProductCategory pc 
                ON ps.productcategoryid = pc.productcategoryid
            LEFT JOIN adventureworks_staging.stg_production_productmodel pm 
                ON p.productmodelid = pm.productmodelid;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. EXPIRE logic (Now also detects changes in Reorder levels)
        expire_sql = f"""
            UPDATE DimProduct
            SET IsCurrent = FALSE, 
                ValidToDate = '{yesterday}', 
                EffectiveEndDate = '{yesterday}'
            FROM adventureworks_staging.stg_dim_product_upsert s
            WHERE DimProduct.ProductID = s.ProductID
            AND DimProduct.IsCurrent = TRUE 
            AND (
                DimProduct.ListPrice != s.ListPrice OR 
                DimProduct.Cost != s.Cost OR 
                DimProduct.Category != s.Category OR 
                DimProduct.ProductStatus != s.ProductStatus OR
                DimProduct.ReorderPoint != s.ReorderPoint OR
                DimProduct.SafetyStockLevel != s.SafetyStockLevel
            );
        """
        starrocks_hook.run(expire_sql)

        # 4. INSERT logic
        insert_sql = f"""
            INSERT INTO DimProduct (
                ProductKey, ProductID, ProductName, SKU, Category, SubCategory, 
                Brand, ListPrice, Cost, ProductStatus, Color, Size, Weight,
                ReorderPoint, SafetyStockLevel,
                ValidFromDate, ValidToDate, IsCurrent, 
                SourceUpdateDate, EffectiveStartDate, EffectiveEndDate
            )
            SELECT 
                murmur_hash3_32(CONCAT(CAST(s.ProductID AS CHAR), '{proc_date}')),
                s.ProductID, s.ProductName, s.SKU, s.Category, s.SubCategory,
                s.Brand, s.ListPrice, s.Cost, s.ProductStatus, s.Color, s.Size, s.Weight,
                s.ReorderPoint, s.SafetyStockLevel,
                '{proc_date}', NULL, TRUE, 
                s.SourceUpdateDate, '{proc_date}', NULL
            FROM adventureworks_staging.stg_dim_product_upsert s
            LEFT JOIN DimProduct d ON s.ProductID = d.ProductID AND d.IsCurrent = TRUE
            WHERE d.ProductID IS NULL;
        """
        starrocks_hook.run(insert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_product_data_into_dimproduct_and_upload_to_starrocks()