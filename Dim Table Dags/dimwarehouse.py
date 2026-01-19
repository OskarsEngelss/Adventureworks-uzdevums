import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_warehouse_data_into_dimwarehouse_and_upload_to_starrocks",
    schedule=None, #schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimwarehouse", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_warehouse_data_into_dimwarehouse_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()
        yesterday = pendulum.now().subtract(days=1).to_date_string()

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_warehouse_upsert WHERE WarehouseID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = f"""
            INSERT INTO adventureworks_staging.stg_dim_warehouse_upsert
            SELECT 
                locationid AS WarehouseID,
                name AS WarehouseName,
                'Main Plant' AS Location, 
                CASE 
                    WHEN costrate > 0 THEN 'Manufacturing Center' 
                    ELSE 'Storage Facility' 
                END AS WarehouseType,
                1 AS ManagerKey, -- Placeholder linking to DimEmployee
                CURRENT_DATE() AS SourceUpdateDate
            FROM adventureworks_staging.stg_production_location;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. EXPIRE logic (SCD2 - Detects changes to track history)
        expire_sql = f"""
            UPDATE DimWarehouse
            SET IsCurrent = FALSE, 
                ValidToDate = '{yesterday}'
            FROM adventureworks_staging.stg_dim_warehouse_upsert s
            WHERE DimWarehouse.WarehouseID = s.WarehouseID
            AND DimWarehouse.IsCurrent = TRUE 
            AND (
                DimWarehouse.WarehouseName != s.WarehouseName OR 
                DimWarehouse.WarehouseType != s.WarehouseType
            );
        """
        starrocks_hook.run(expire_sql)

        # 4. INSERT logic (Inserts brand new records OR the new 'Current' version of changed ones)
        insert_sql = f"""
            INSERT INTO DimWarehouse (
                WarehouseKey, ValidFromDate, WarehouseID, WarehouseName, 
                Location, WarehouseType, ManagerKey, ValidToDate, IsCurrent
            )
            SELECT 
                murmur_hash3_32(CONCAT(CAST(s.WarehouseID AS CHAR), '{proc_date}')),
                '{proc_date}', 
                s.WarehouseID, 
                s.WarehouseName, 
                s.Location, 
                s.WarehouseType, 
                s.ManagerKey, 
                NULL, 
                TRUE
            FROM adventureworks_staging.stg_dim_warehouse_upsert s
            LEFT JOIN DimWarehouse d 
                ON s.WarehouseID = d.WarehouseID AND d.IsCurrent = TRUE
            WHERE d.WarehouseID IS NULL;
        """
        starrocks_hook.run(insert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_warehouse_data_into_dimwarehouse_and_upload_to_starrocks()