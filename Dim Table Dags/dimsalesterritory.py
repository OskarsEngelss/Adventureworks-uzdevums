import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_sales_territory_data_into_dimsalesterritory_and_upload_to_starrocks",
    schedule=None, #schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimsalesterritory", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_sales_territory_data_into_dimsalesterritory_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_sales_territory_upsert WHERE TerritoryID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = """
            INSERT INTO adventureworks_staging.stg_dim_sales_territory_upsert
            SELECT 
                st.territoryid AS TerritoryID,
                st.name AS TerritoryName,
                st.`group` AS SalesRegion,
                st.countryregioncode AS Country,
                COALESCE(CONCAT(p.firstname, ' ', p.lastname), 'No Manager') AS Manager,
                st.salesytd AS SalesTarget, -- Using SalesYTD as a proxy for current target
                CURRENT_DATE() AS SourceUpdateDate
            FROM adventureworks_staging.stg_sales_salesterritory st
            LEFT JOIN adventureworks_staging.stg_sales_salesperson sp 
                ON st.territoryid = sp.territoryid
            LEFT JOIN adventureworks_staging.stg_person_person p 
                ON sp.businessentityid = p.businessentityid;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. UPSERT Logic (SCD Type 1)
        upsert_sql = """
            INSERT INTO DimSalesTerritory (
                TerritoryKey, TerritoryID, TerritoryName, SalesRegion, 
                Country, Manager, SalesTarget
            )
            SELECT 
                murmur_hash3_32(CAST(s.TerritoryID AS CHAR)) AS TerritoryKey,
                s.TerritoryID, 
                s.TerritoryName, 
                s.SalesRegion, 
                s.Country, 
                s.Manager, 
                s.SalesTarget
            FROM adventureworks_staging.stg_dim_sales_territory_upsert s;
        """
        starrocks_hook.run(upsert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_sales_territory_data_into_dimsalesterritory_and_upload_to_starrocks()