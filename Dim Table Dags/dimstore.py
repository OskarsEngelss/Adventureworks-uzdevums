import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_store_data_into_dimstore_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimstore", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_store_data_into_dimstore_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()
        yesterday = pendulum.now().subtract(days=1).to_date_string()

        # --- STEP 1: CORRECTED QUARANTINE ---
        # Using businessentityid and name to match stg_sales_store
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimStore',
                COALESCE(CAST(stg.businessentityid AS CHAR), 'UNKNOWN'),
                'Missing Store Name or Entity ID',
                1, 0, 0,
                json_object(
                    'storeid', CAST(stg.businessentityid AS CHAR),
                    'name', stg.name,
                    'salespersonid', CAST(stg.salespersonid AS CHAR)
                )
            FROM adventureworks_staging.stg_sales_store stg
            WHERE stg.businessentityid IS NULL OR stg.name IS NULL;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: CLEAN STAGING & TRANSFORM ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks_staging.stg_dim_store_upsert;")

            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_store_upsert
                SELECT 
                    s.businessentityid AS StoreID,
                    s.name AS StoreName,
                    s.businessentityid AS StoreNumber,
                    a.addressline1 AS Address,
                    a.city,
                    sp.name AS StateProvince,
                    cr.name AS Country,
                    a.postalcode,
                    st.`group` AS Region,
                    st.name AS Territory,
                    'Retail' AS StoreType,
                    'Open' AS StoreStatus,
                    COALESCE(CONCAT(p.firstname, ' ', p.lastname), 'Unknown') AS ManagerName,
                    CAST(s.modifieddate AS DATE) AS OpeningDate,
                    0 AS SquareFootage,
                    CURRENT_DATE() AS SourceUpdateDate
                FROM adventureworks_staging.stg_sales_store s
                LEFT JOIN adventureworks_staging.stg_person_businessentityaddress bea 
                    ON s.businessentityid = bea.businessentityid
                LEFT JOIN adventureworks_staging.stg_person_address a 
                    ON bea.addressid = a.addressid
                LEFT JOIN adventureworks_staging.stg_person_stateprovince sp 
                    ON a.stateprovinceid = sp.stateprovinceid
                LEFT JOIN adventureworks_staging.stg_person_countryregion cr 
                    ON sp.countryregioncode = cr.countryregioncode
                LEFT JOIN adventureworks_staging.stg_sales_salesperson ss 
                    ON s.salespersonid = ss.businessentityid
                LEFT JOIN adventureworks_staging.stg_sales_salesterritory st 
                    ON ss.territoryid = st.territoryid
                LEFT JOIN adventureworks_staging.stg_person_person p 
                    ON s.salespersonid = p.businessentityid;
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: SCD TYPE 2 EXPIRE ---
            expire_sql = f"""
                UPDATE adventureworks.DimStore
                SET IsCurrent = FALSE, 
                    ValidToDate = '{yesterday}'
                FROM adventureworks_staging.stg_dim_store_upsert s
                WHERE DimStore.StoreID = s.StoreID
                AND DimStore.IsCurrent = TRUE 
                AND (
                    DimStore.Address != s.Address OR 
                    DimStore.ManagerName != s.ManagerName OR
                    DimStore.Territory != s.Territory
                );
            """
            starrocks_hook.run(expire_sql)

            # --- STEP 4: SCD TYPE 2 INSERT ---
            insert_sql = """
                INSERT INTO adventureworks.DimStore (
                    StoreKey, ValidFromDate, StoreID, StoreName, StoreNumber, 
                    Address, City, StateProvince, Country, PostalCode, 
                    Region, Territory, StoreType, StoreStatus, ManagerName, 
                    OpeningDate, SquareFootage, ValidToDate, IsCurrent, SourceUpdateDate
                )
                SELECT 
                    murmur_hash3_32(CONCAT(CAST(s.StoreID AS CHAR), CAST(s.OpeningDate AS VARCHAR))),
                    s.OpeningDate,
                    s.StoreID, s.StoreName, s.StoreNumber,
                    s.Address, s.City, s.StateProvince, s.Country, s.PostalCode,
                    s.Region, s.Territory, s.StoreType, s.StoreStatus, s.ManagerName,
                    s.OpeningDate, s.SquareFootage, NULL, TRUE, s.SourceUpdateDate
                FROM adventureworks_staging.stg_dim_store_upsert s
                LEFT JOIN adventureworks.DimStore d 
                    ON s.StoreID = d.StoreID AND d.IsCurrent = TRUE
                WHERE d.StoreID IS NULL;
            """
            starrocks_hook.run(insert_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimStore",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure in DimStore synchronization"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_store_data_into_dimstore_and_upload_to_starrocks()