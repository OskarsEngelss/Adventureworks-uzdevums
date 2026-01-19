import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_promotion_data_into_dimpromotion_and_upload_to_starrocks",
    schedule=None, #schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimpromotion", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_promotion_data_into_dimpromotion_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_promotion_upsert WHERE PromotionID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = """
            INSERT INTO adventureworks_staging.stg_dim_promotion_upsert
            SELECT 
                specialofferid AS PromotionID,
                description AS PromotionName,
                description AS PromotionDescription,
                type AS PromotionType,
                discountpct AS DiscountPercentage,
                0 AS DiscountAmount, -- AW uses Percentage mostly
                startdate AS StartDate,
                enddate AS EndDate,
                CASE WHEN CURRENT_DATE() BETWEEN startdate AND enddate THEN TRUE ELSE FALSE END AS IsActive,
                'Active' AS PromotionStatus,
                0 AS CampaignID, -- Placeholder
                CURRENT_DATE() AS SourceUpdateDate
            FROM adventureworks_staging.stg_sales_specialoffer;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. UPSERT Logic (SCD Type 1 - Just overwrite changes)
        upsert_sql = """
            INSERT INTO DimPromotion (
                PromotionKey, PromotionID, PromotionName, PromotionDescription, 
                PromotionType, DiscountPercentage, DiscountAmount, StartDate, 
                EndDate, IsActive, PromotionStatus, CampaignID, SourceUpdateDate
            )
            SELECT 
                murmur_hash3_32(CAST(s.PromotionID AS CHAR)) AS PromotionKey,
                s.PromotionID, s.PromotionName, s.PromotionDescription, 
                s.PromotionType, s.DiscountPercentage, s.DiscountAmount, s.StartDate, 
                s.EndDate, s.IsActive, s.PromotionStatus, s.CampaignID, s.SourceUpdateDate
            FROM adventureworks_staging.stg_dim_promotion_upsert s;
        """
        starrocks_hook.run(upsert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_promotion_data_into_dimpromotion_and_upload_to_starrocks()