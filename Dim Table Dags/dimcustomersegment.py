import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_customer_segment_data_into_dimcustomersegment_and_upload_to_starrocks",
    schedule=None, #schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimcustomersegment", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_customer_segment_data_into_dimcustomersegment_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_customer_segment_upsert WHERE SegmentID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = """
            INSERT INTO adventureworks_staging.stg_dim_customer_segment_upsert
            SELECT 
                SegmentID,
                SegmentName,
                SegmentDescription,
                DiscountTierStart,
                DiscountTierEnd,
                CURRENT_DATE() AS SourceUpdateDate
            FROM (
                SELECT 1 as SegmentID, 'Budget' as SegmentName, 'Price-sensitive customers' as SegmentDescription, 0.00 as DiscountTierStart, 0.05 as DiscountTierEnd
                UNION ALL SELECT 2, 'Standard', 'Regular retail customers' as SegmentDescription, 0.05, 0.10
                UNION ALL SELECT 3, 'Premium', 'High-value frequent shoppers' as SegmentDescription, 0.10, 0.20
                UNION ALL SELECT 4, 'VIP', 'Top-tier corporate or elite shoppers' as SegmentDescription, 0.20, 0.40
            ) src;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. UPSERT Logic (SCD Type 1 - Overwrite)
        upsert_sql = """
            INSERT INTO DimCustomerSegment (
                SegmentKey, SegmentID, SegmentName, SegmentDescription, 
                DiscountTierStart, DiscountTierEnd
            )
            SELECT 
                murmur_hash3_32(CAST(s.SegmentID AS CHAR)) AS SegmentKey,
                s.SegmentID, 
                s.SegmentName, 
                s.SegmentDescription, 
                s.DiscountTierStart, 
                s.DiscountTierEnd
            FROM adventureworks_staging.stg_dim_customer_segment_upsert s;
        """
        starrocks_hook.run(upsert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_customer_segment_data_into_dimcustomersegment_and_upload_to_starrocks()