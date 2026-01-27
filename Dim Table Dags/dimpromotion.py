import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_promotion_data_into_dimpromotion_and_upload_to_starrocks",
    schedule=None,
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
        proc_date = pendulum.now().to_date_string()

        # --- STEP 1: QUARANTINE (Logic Errors) ---
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimPromotion',
                COALESCE(CAST(stg.specialofferid AS CHAR), 'UNKNOWN'),
                CASE 
                    WHEN stg.startdate > stg.enddate THEN 'Start Date After End Date'
                    WHEN stg.discountpct < 0 THEN 'Negative Discount'
                    WHEN stg.specialofferid IS NULL THEN 'Missing ID'
                END,
                1, 0, 0,
                json_object(
                    'promotionid', CAST(stg.specialofferid AS CHAR),
                    'description', stg.description, -- Changed from 'name' to 'description'
                    'startdate', CAST(stg.startdate AS CHAR),
                    'enddate', CAST(stg.enddate AS CHAR)
                )
            FROM adventureworks_staging.stg_sales_specialoffer stg
            WHERE stg.specialofferid IS NULL 
               OR stg.startdate > stg.enddate 
               OR stg.discountpct < 0;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: CLEAN & TRANSFORM INTO STAGING ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks_staging.stg_dim_promotion_upsert;")

            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_promotion_upsert
                SELECT 
                    specialofferid AS PromotionID,
                    description AS PromotionName,
                    description AS PromotionDescription,
                    type AS PromotionType,
                    discountpct AS DiscountPercentage,
                    0 AS DiscountAmount,
                    startdate AS StartDate,
                    enddate AS EndDate,
                    CASE WHEN CURRENT_DATE() BETWEEN startdate AND enddate THEN TRUE ELSE FALSE END AS IsActive,
                    'Active' AS PromotionStatus,
                    0 AS CampaignID,
                    CURRENT_DATE() AS SourceUpdateDate
                FROM adventureworks_staging.stg_sales_specialoffer
                WHERE startdate <= enddate AND discountpct >= 0;
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: UPSERT Logic (SCD Type 1) ---
            # Using INSERT...ON DUPLICATE KEY UPDATE or DELETE+INSERT for SCD1
            # For DimPromotion, we'll use your logic but ensure we clear old keys
            starrocks_hook.run("DELETE FROM adventureworks.DimPromotion WHERE PromotionID IS NOT NULL;")

            upsert_sql = f"""
                INSERT INTO adventureworks.DimPromotion (
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

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimPromotion",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_promotion_data_into_dimpromotion_and_upload_to_starrocks()