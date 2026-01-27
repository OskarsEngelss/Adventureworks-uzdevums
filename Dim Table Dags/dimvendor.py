import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_vendor_data_into_dimvendor_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimvendor", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_vendor_data_into_dimvendor_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()
        yesterday = pendulum.now().subtract(days=1).to_date_string()

        # --- STEP 1: QUARANTINE ---
        # Catching invalid ratings or missing IDs before they hit the warehouse
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimVendor',
                COALESCE(CAST(stg.businessentityid AS CHAR), 'UNKNOWN'),
                'Invalid Vendor Rating or Missing ID',
                1, 0, 0,
                json_object(
                    'vendorid', CAST(stg.businessentityid AS CHAR),
                    'vendorname', stg.name,
                    'rating', CAST(stg.creditrating AS CHAR)
                )
            FROM adventureworks_staging.stg_purchasing_vendor stg
            WHERE stg.businessentityid IS NULL 
               OR stg.creditrating NOT BETWEEN 1 AND 5;
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: CLEAN STAGING & TRANSFORM ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks_staging.stg_dim_vendor_upsert;")

            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_vendor_upsert
                SELECT 
                    VendorID, VendorName, ContactPerson, Email, Phone, 
                    Address, City, Country, VendorRating, OnTimeDeliveryRate, 
                    QualityScore, PaymentTerms, VendorStatus, SourceUpdateDate
                FROM (
                    SELECT 
                        v.businessentityid AS VendorID,
                        v.name AS VendorName,
                        COALESCE(CONCAT(p.firstname, ' ', p.lastname), 'N/A') AS ContactPerson,
                        COALESCE(ea.emailaddress, 'N/A') AS Email,
                        COALESCE(ph.phonenumber, 'N/A') AS Phone,
                        a.addressline1 AS Address,
                        a.city AS City,
                        cr.name AS Country,
                        v.creditrating AS VendorRating,
                        0.00 AS OnTimeDeliveryRate,
                        v.creditrating * 20 AS QualityScore,
                        'Standard' AS PaymentTerms,
                        CASE WHEN v.activeflag = 1 THEN 'Active' ELSE 'Inactive' END AS VendorStatus,
                        CURRENT_DATE() AS SourceUpdateDate,
                        ROW_NUMBER() OVER(PARTITION BY v.businessentityid ORDER BY bea.addressid DESC) as rnk
                    FROM adventureworks_staging.stg_purchasing_vendor v
                    LEFT JOIN adventureworks_staging.stg_person_businessentityaddress bea 
                        ON v.businessentityid = bea.businessentityid
                    LEFT JOIN adventureworks_staging.stg_person_address a 
                        ON bea.addressid = a.addressid
                    LEFT JOIN adventureworks_staging.stg_person_stateprovince sp 
                        ON a.stateprovinceid = sp.stateprovinceid
                    LEFT JOIN adventureworks_staging.stg_person_countryregion cr 
                        ON sp.countryregioncode = cr.countryregioncode
                    LEFT JOIN adventureworks_staging.stg_person_businessentitycontact bec 
                        ON v.businessentityid = bec.businessentityid
                    LEFT JOIN adventureworks_staging.stg_person_person p 
                        ON bec.personid = p.businessentityid
                    LEFT JOIN adventureworks_staging.stg_person_emailaddress ea 
                        ON p.businessentityid = ea.businessentityid
                    LEFT JOIN adventureworks_staging.stg_person_personphone ph 
                        ON p.businessentityid = ph.businessentityid
                ) t
                WHERE t.rnk = 1;
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: SCD TYPE 2 EXPIRE ---
            expire_sql = f"""
                UPDATE adventureworks.DimVendor
                SET IsCurrent = FALSE, 
                    ValidToDate = '{yesterday}'
                FROM adventureworks_staging.stg_dim_vendor_upsert s
                WHERE DimVendor.VendorID = s.VendorID
                AND DimVendor.IsCurrent = TRUE 
                AND (
                    DimVendor.VendorRating != s.VendorRating OR 
                    DimVendor.QualityScore != s.QualityScore OR 
                    DimVendor.VendorStatus != s.VendorStatus
                );
            """
            starrocks_hook.run(expire_sql)

            # --- STEP 4: SCD TYPE 2 INSERT ---
            insert_sql = f"""
                INSERT INTO adventureworks.DimVendor (
                    VendorKey, ValidFromDate, VendorID, VendorName, ContactPerson, 
                    Email, Phone, Address, City, Country, VendorRating, 
                    OnTimeDeliveryRate, QualityScore, PaymentTerms, VendorStatus, 
                    ValidToDate, IsCurrent, SourceUpdateDate
                )
                SELECT 
                    murmur_hash3_32(CONCAT(CAST(s.VendorID AS CHAR), '{proc_date}')),
                    '{proc_date}', s.VendorID, s.VendorName, s.ContactPerson,
                    s.Email, s.Phone, s.Address, s.City, s.Country, s.VendorRating,
                    s.OnTimeDeliveryRate, s.QualityScore, s.PaymentTerms, s.VendorStatus,
                    NULL, TRUE, s.SourceUpdateDate
                FROM adventureworks_staging.stg_dim_vendor_upsert s
                LEFT JOIN adventureworks.DimVendor d 
                    ON s.VendorID = d.VendorID AND d.IsCurrent = TRUE
                WHERE d.VendorID IS NULL;
            """
            starrocks_hook.run(insert_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimVendor",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_vendor_data_into_dimvendor_and_upload_to_starrocks()