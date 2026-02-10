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
            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_product_upsert (
                    ProductID, ProductName, SKU, Category, SubCategory, 
                    Brand, ListPrice, Cost, ProductStatus, Color, Size, Weight,
                    ReorderPoint, SafetyStockLevel, SourceUpdateDate
                )
                SELECT 
                    p.productid AS ProductID,
                    p.name AS ProductName,
                    p.productnumber AS SKU,
                    COALESCE(pc.name, 'Components') AS Category, -- Fix: Map NULL Category
                    COALESCE(ps.name, 'Non-Saleable') AS SubCategory, -- Fix: Map NULL SubCategory
                    COALESCE(p.name, 'Generic') AS Brand,
                    p.listprice AS ListPrice,
                    p.standardcost AS Cost,
                    CASE 
                        WHEN p.sellenddate IS NOT NULL THEN 'Discontinued'
                        ELSE 'Active'
                    END AS ProductStatus,
                    p.color AS Color,
                    p.size AS Size,
                    p.weight AS Weight,
                    p.reorderpoint AS ReorderPoint,
                    p.safetystocklevel AS SafetyStockLevel,
                    CURRENT_DATE() AS SourceUpdateDate
                FROM adventureworks_staging.stg_production_product p
                LEFT JOIN adventureworks_staging.stg_production_productsubcategory ps 
                    ON p.productsubcategoryid = ps.productsubcategoryid
                LEFT JOIN adventureworks_staging.stg_production_productcategory pc 
                    ON ps.productcategoryid = pc.productcategoryid
                UNION ALL
                -- Handling the Unknown/Manual Record (Ensuring Category is not NULL here either)
                SELECT 0, 'Unknown Product', 'N/A', 'Unknown', 'Unknown', 'N/A', 0, 0, 'Active', NULL, NULL, NULL, NULL, NULL, CURRENT_DATE();
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
                INSERT INTO adventureworks.DimProduct (
                    ProductKey, ProductID, ProductName, SKU, Category, SubCategory,
                    Brand, ListPrice, Cost, ProductStatus, Color, Size, Weight,
                    ReorderPoint, SafetyStockLevel,
                    ValidFromDate, ValidToDate, IsCurrent, 
                    SourceUpdateDate, EffectiveStartDate, EffectiveEndDate
                )
                SELECT 
                    CASE 
                        WHEN s.ProductID = 0 THEN 0 
                        ELSE murmur_hash3_32(CONCAT(CAST(s.ProductID AS CHAR), '{proc_date}')) 
                    END AS ProductKey,
                    s.ProductID, s.ProductName, s.SKU, s.Category, s.SubCategory,
                    s.Brand, s.ListPrice, s.Cost, s.ProductStatus, s.Color, s.Size, s.Weight,
                    s.ReorderPoint, s.SafetyStockLevel,
                    '{proc_date}', 
                    CASE WHEN s.RowRank = 1 THEN NULL ELSE s.SourceUpdateDate END, 
                    CASE WHEN s.RowRank = 1 THEN TRUE ELSE FALSE END, 
                    s.SourceUpdateDate, '{proc_date}', 
                    CASE WHEN s.RowRank = 1 THEN NULL ELSE s.SourceUpdateDate END
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY SourceUpdateDate DESC) as RowRank
                    FROM adventureworks_staging.stg_dim_product_upsert
                ) s
                LEFT JOIN adventureworks.DimProduct d ON s.ProductID = d.ProductID AND d.IsCurrent = TRUE
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