import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_warehouse_data_into_dimwarehouse_and_upload_to_starrocks",
    schedule=None,
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

        # --- STEP 1: QUARANTINE ---
        # Catching missing IDs or Names before processing
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimWarehouse',
                COALESCE(CAST(stg.locationid AS CHAR), 'UNKNOWN'),
                'Missing Location ID or Name',
                1, 0, 0,
                json_object(
                    'locationid', CAST(stg.locationid AS CHAR), 
                    'name', stg.name
                )
            FROM adventureworks_staging.stg_production_location stg
            WHERE stg.locationid IS NULL OR stg.name IS NULL;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: CLEAN STAGING & TRANSFORM ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks_staging.stg_dim_warehouse_upsert;")

            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_warehouse_upsert
                WITH MainHQ AS (
                    -- Simplified: Just get the lowest ID from the store table
                    -- In AdventureWorks, this is always the Corporate HQ.
                    SELECT MIN(businessentityid) as hq_id
                    FROM adventureworks_staging.stg_sales_store
                )
                SELECT 
                    stg.locationid AS WarehouseID,
                    stg.name AS WarehouseName,
                    addr.City AS Location, 
                    CASE 
                        WHEN stg.costrate > 0 THEN 'Manufacturing Center' 
                        ELSE 'Storage Facility' 
                    END AS WarehouseType,
                    COALESCE(e.EmployeeKey, 0) AS ManagerKey,
                    CURRENT_DATE() AS SourceUpdateDate
                FROM adventureworks_staging.stg_production_location stg
                -- Join the HQ ID
                CROSS JOIN MainHQ
                LEFT JOIN adventureworks_staging.stg_person_businessentityaddress bea 
                    ON bea.BusinessEntityID = MainHQ.hq_id
                LEFT JOIN adventureworks_staging.stg_person_address addr 
                    ON bea.AddressID = addr.AddressID
                -- Manager mapping
                LEFT JOIN adventureworks.DimEmployee e 
                    ON (e.JobTitle LIKE CONCAT('%WC', CAST(stg.locationid AS CHAR), '%')
                        OR e.JobTitle LIKE CONCAT('%', stg.name, '%'))
                    AND e.IsCurrent = 1
                WHERE stg.locationid IS NOT NULL;
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: SCD TYPE 2 EXPIRE ---
            expire_sql = f"""
                UPDATE adventureworks.DimWarehouse
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

            # --- STEP 4: SCD TYPE 2 INSERT ---
            insert_sql = f"""
                INSERT INTO adventureworks.DimWarehouse (
                    WarehouseKey, ValidFromDate, WarehouseID, WarehouseName, 
                    Location, WarehouseType, ManagerKey, ValidToDate, IsCurrent
                )
                SELECT 
                    murmur_hash3_32(CAST(s.WarehouseID AS CHAR)),
                    '{proc_date}', 
                    s.WarehouseID, 
                    s.WarehouseName, 
                    s.Location, 
                    s.WarehouseType, 
                    s.ManagerKey, 
                    CASE WHEN s.RowRank = 1 THEN NULL ELSE '{proc_date}' END, 
                    CASE WHEN s.RowRank = 1 THEN TRUE ELSE FALSE END
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY WarehouseID ORDER BY WarehouseID) as RowRank
                    FROM adventureworks_staging.stg_dim_warehouse_upsert
                ) s
                LEFT JOIN adventureworks.DimWarehouse d 
                    ON s.WarehouseID = d.WarehouseID AND d.IsCurrent = TRUE
                WHERE d.WarehouseID IS NULL;
            """
            starrocks_hook.run(insert_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimWarehouse",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_warehouse_data_into_dimwarehouse_and_upload_to_starrocks()