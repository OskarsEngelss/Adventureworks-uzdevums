import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_customer_data_into_dimcustomer_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    max_active_tasks=3,
    tags=["starrocks", "dimcustomer", "load", "transform", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_customer_data_into_dimcustomer_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()
        yesterday = pendulum.now().subtract(days=1).to_date_string()

        # --- STEP 1: QUARANTINE (Invalid Emails or Missing Keys) ---
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimCustomer',
                COALESCE(CAST(stg.customerid AS CHAR), 'UNKNOWN'),
                CASE 
                    WHEN stg.customerid IS NULL THEN 'Missing Natural Key'
                    WHEN e.emailaddress NOT LIKE '%@%' THEN 'Invalid Email Format'
                    ELSE 'Missing Required Info'
                END,
                1, 0, 0,
                json_object(
                    'customerid', stg.customerid,
                    'email', e.emailaddress
                )
            FROM adventureworks_staging.stg_sales_customer stg
            LEFT JOIN adventureworks_staging.stg_person_emailaddress e 
                ON stg.personid = e.businessentityid
            WHERE stg.customerid IS NULL OR e.emailaddress NOT LIKE '%@%';
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: CLEAN & TRANSFORM INTO STAGING UPSERT ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks_staging.stg_dim_customer_upsert;")

            load_staging_sql = f"""
                INSERT INTO adventureworks_staging.stg_dim_customer_upsert
                WITH CustomerSalesStats AS (
                    SELECT 
                        customerid,
                        MIN(orderdate) as FirstPurchaseDate,
                        SUM(totaldue) as TotalLifetimeSpend
                    FROM adventureworks_staging.stg_sales_salesorderheader
                    GROUP BY customerid
                ),
                CustomerBase AS (
                    SELECT 
                        c.customerid,
                        c.personid,
                        c.storeid,
                        COALESCE(
                            NULLIF(CONCAT(COALESCE(p.firstname, ''), ' ', COALESCE(p.lastname, '')), ' '),
                            s.name,
                            CAST(c.customerid AS CHAR)
                        ) AS CustomerName
                    FROM adventureworks_staging.stg_sales_customer c
                    LEFT JOIN adventureworks_staging.stg_person_person p ON c.personid = p.businessentityid
                    LEFT JOIN adventureworks_staging.stg_sales_store s ON c.storeid = s.businessentityid
                )
                SELECT 
                    cb.customerid,
                    cb.CustomerName,
                    e.emailaddress,
                    ph.phonenumber,
                    a.city,
                    sp.name as StateProvince,
                    cr.name as Country,
                    a.postalcode,
                    COALESCE(seg.SegmentName, 'Budget') AS CustomerSegment,
                    CASE WHEN cb.storeid IS NOT NULL THEN 'Corporate' ELSE 'Individual' END as CustomerType,
                    'Active' as AccountStatus, 
                    0.00 as CreditLimit, 
                    0.00 as AnnualIncome,
                    COALESCE(TIMESTAMPDIFF(YEAR, stats.FirstPurchaseDate, CURRENT_DATE()), 0) as YearsSinceFirstPurchase,
                    CURRENT_DATE() as SourceUpdateDate
                FROM CustomerBase cb
                LEFT JOIN CustomerSalesStats stats ON cb.customerid = stats.customerid
                LEFT JOIN adventureworks.DimCustomerSegment seg 
                    ON stats.TotalLifetimeSpend >= (seg.DiscountTierStart * 10000)
                   AND stats.TotalLifetimeSpend < (seg.DiscountTierEnd * 10000)
                LEFT JOIN adventureworks_staging.stg_person_emailaddress e ON cb.personid = e.businessentityid
                LEFT JOIN adventureworks_staging.stg_person_personphone ph ON cb.personid = ph.businessentityid
                LEFT JOIN adventureworks_staging.stg_person_businessentityaddress bea 
                    ON (cb.personid = bea.businessentityid OR cb.storeid = bea.businessentityid)
                LEFT JOIN adventureworks_staging.stg_person_address a ON bea.addressid = a.addressid
                LEFT JOIN adventureworks_staging.stg_person_stateprovince sp ON a.stateprovinceid = sp.stateprovinceid
                LEFT JOIN adventureworks_staging.stg_person_countryregion cr ON sp.countryregioncode = cr.countryregioncode
                WHERE cb.customerid IS NOT NULL AND (e.emailaddress LIKE '%@%' OR e.emailaddress IS NULL)
                GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15;
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: SCD TYPE 2 EXPIRE ---
            expire_sql = f"""
                UPDATE adventureworks.DimCustomer
                SET IsCurrent = FALSE, 
                    ValidToDate = '{yesterday}', 
                    EffectiveEndDate = '{yesterday}'
                FROM adventureworks_staging.stg_dim_customer_upsert s
                WHERE DimCustomer.CustomerID = s.CustomerID
                AND DimCustomer.IsCurrent = TRUE 
                AND (DimCustomer.Email != s.Email OR DimCustomer.City != s.City OR DimCustomer.CustomerSegment != s.CustomerSegment);
            """
            starrocks_hook.run(expire_sql)

            # --- STEP 4: SCD TYPE 2 INSERT ---
            insert_sql = f"""
                INSERT INTO adventureworks.DimCustomer (
                    CustomerKey, ValidFromDate, CustomerID, CustomerName, Email, Phone, 
                    City, StateProvince, Country, PostalCode, CustomerSegment, 
                    CustomerType, AccountStatus, CreditLimit, AnnualIncome, 
                    YearsSinceFirstPurchase, ValidToDate, IsCurrent, 
                    SourceUpdateDate, EffectiveStartDate, EffectiveEndDate
                )
                SELECT 
                    murmur_hash3_32(CONCAT(CAST(s.CustomerID AS CHAR), '{proc_date}')),
                    '{proc_date}', s.CustomerID, s.CustomerName, s.Email, s.Phone, s.City, 
                    s.StateProvince, s.Country, s.PostalCode, s.CustomerSegment, s.CustomerType, 
                    s.AccountStatus, s.CreditLimit, s.AnnualIncome, s.YearsSinceFirstPurchase, 
                    NULL, TRUE, s.SourceUpdateDate, '{proc_date}', NULL
                FROM adventureworks_staging.stg_dim_customer_upsert s
                LEFT JOIN adventureworks.DimCustomer d ON s.CustomerID = d.CustomerID AND d.IsCurrent = TRUE
                WHERE NOT EXISTS (
                    SELECT 1 FROM adventureworks.DimCustomer check_d 
                    WHERE check_d.CustomerID = s.CustomerID 
                    AND check_d.IsCurrent = TRUE
                    -- This ensures we only skip if the current record is IDENTICAL to staging
                    AND check_d.Email = s.Email 
                    AND check_d.City = s.City 
                    AND check_d.CustomerSegment = s.CustomerSegment
                )
            """
            starrocks_hook.run(insert_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimCustomer",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_customer_data_into_dimcustomer_and_upload_to_starrocks()