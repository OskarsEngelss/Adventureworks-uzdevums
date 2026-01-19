import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_store_data_into_dimstore_and_upload_to_starrocks",
    schedule=None, #schedule="@daily",
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

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_store_upsert WHERE StoreID IS NOT NULL;")

       # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = f"""
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
                st.`group` AS Region, -- Fixed: Added backticks around reserved keyword
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
            -- Joining to SalesPerson to get their Territory
            LEFT JOIN adventureworks_staging.stg_sales_salesperson ss 
                ON s.salespersonid = ss.businessentityid
            -- Joining to Territory to get the Region (group) and Territory Name
            LEFT JOIN adventureworks_staging.stg_sales_salesterritory st 
                ON ss.territoryid = st.territoryid
            LEFT JOIN adventureworks_staging.stg_person_person p 
                ON s.salespersonid = p.businessentityid;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. EXPIRE logic (Change detection on Address, Region, Territory, Manager, Status)
        expire_sql = f"""
            UPDATE DimStore
            SET IsCurrent = FALSE, 
                ValidToDate = '{yesterday}'
            FROM adventureworks_staging.stg_dim_store_upsert s
            WHERE DimStore.StoreID = s.StoreID
            AND DimStore.IsCurrent = TRUE 
            AND (
                DimStore.Address != s.Address OR 
                DimStore.Region != s.Region OR 
                DimStore.Territory != s.Territory OR 
                DimStore.ManagerName != s.ManagerName OR
                DimStore.StoreStatus != s.StoreStatus
            );
        """
        starrocks_hook.run(expire_sql)

        # 4. INSERT logic
        insert_sql = f"""
            INSERT INTO DimStore (
                StoreKey, ValidFromDate, StoreID, StoreName, StoreNumber, 
                Address, City, StateProvince, Country, PostalCode, 
                Region, Territory, StoreType, StoreStatus, ManagerName, 
                OpeningDate, SquareFootage, ValidToDate, IsCurrent, SourceUpdateDate
            )
            SELECT 
                murmur_hash3_32(CONCAT(CAST(s.StoreID AS CHAR), '{proc_date}')),
                '{proc_date}', s.StoreID, s.StoreName, s.StoreNumber,
                s.Address, s.City, s.StateProvince, s.Country, s.PostalCode,
                s.Region, s.Territory, s.StoreType, s.StoreStatus, s.ManagerName,
                s.OpeningDate, s.SquareFootage, NULL, TRUE, s.SourceUpdateDate
            FROM adventureworks_staging.stg_dim_store_upsert s
            LEFT JOIN DimStore d ON s.StoreID = d.StoreID AND d.IsCurrent = TRUE
            WHERE d.StoreID IS NULL;
        """
        starrocks_hook.run(insert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_store_data_into_dimstore_and_upload_to_starrocks()