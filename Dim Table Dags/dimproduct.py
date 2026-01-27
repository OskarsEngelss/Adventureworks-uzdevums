import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_product_data_into_dimproduct_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimproduct", "load", "adventureworks"]
)
def extract_transform_combine_product_data_into_dimproduct_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()
        yesterday = pendulum.now().subtract(days=1).to_date_string()

        # --- STEP 1: QUARANTINE BAD DATA (Fixed column names: FailureReason, FailedData) ---
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimProduct',
                CAST(stg.productid AS CHAR),
                CASE 
                    WHEN stg.listprice < 0 THEN 'Negative List Price'
                    WHEN stg.standardcost > stg.listprice THEN 'Cost Exceeds List Price'
                    WHEN stg.productid IS NULL THEN 'Missing ProductID'
                END,
                1, 0, 0,
                json_object(
                    'productid', stg.productid,
                    'sku', stg.productnumber,
                    'listprice', stg.listprice,
                    'cost', stg.standardcost
                )
            FROM adventureworks_staging.stg_production_product stg
            WHERE stg.productid IS NULL OR stg.listprice < 0 OR stg.standardcost > stg.listprice;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # 1. Clean Staging
            starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_product_upsert WHERE ProductID IS NOT NULL;")

            # 2. TRANSFORM & LOAD (Capitalized Cost and ListPrice)
            load_staging_sql = f"""
                INSERT INTO adventureworks_staging.stg_dim_product_upsert
                SELECT 
                    p.productid AS ProductID,
                    p.name AS ProductName,
                    p.productnumber AS SKU,
                    pc.CategoryName AS Category,
                    ps.name AS SubCategory,
                    pm.name AS Brand,
                    p.listprice AS ListPrice,
                    p.standardcost AS Cost,
                    CASE 
                        WHEN p.sellenddate IS NOT NULL AND p.sellenddate <= CURRENT_DATE() THEN 'Discontinued'
                        WHEN p.sellstartdate > CURRENT_DATE() THEN 'Coming Soon'
                        ELSE 'Active'
                    END AS ProductStatus,
                    p.color AS Color,
                    p.size AS Size,
                    p.weight AS Weight,
                    p.reorderpoint AS ReorderPoint,
                    p.safetystocklevel AS SafetyStockLevel,
                    CURRENT_DATE() as SourceUpdateDate
                FROM adventureworks_staging.stg_production_product p
                LEFT JOIN adventureworks_staging.stg_production_productsubcategory ps 
                    ON p.productsubcategoryid = ps.productsubcategoryid
                LEFT JOIN DimProductCategory pc 
                    ON ps.productcategoryid = pc.productcategoryid
                LEFT JOIN adventureworks_staging.stg_production_productmodel pm 
                    ON p.productmodelid = pm.productmodelid
                WHERE p.productid IS NOT NULL AND p.listprice >= 0 AND p.standardcost <= p.listprice;
            """
            starrocks_hook.run(load_staging_sql)

            # 3. EXPIRE logic (Capitalized Cost and ListPrice)
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

            # 4. INSERT logic (Capitalized Cost and ListPrice)
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

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimProduct",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_product_data_into_dimproduct_and_upload_to_starrocks()