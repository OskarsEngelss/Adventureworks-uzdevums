import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_customer_data_into_dimcustomer_and_upload_to_starrocks",
    schedule=None, #schedule="@daily",
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
    def load_dim_customer_scd2():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()
        yesterday = pendulum.now().subtract(days=1).to_date_string()

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_customer_upsert WHERE CustomerID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        # Integrating DimCustomerSegment for dynamic tiering
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
                        SUBSTRING_INDEX(e_name.emailaddress, '@', 1),
                        CAST(c.customerid AS CHAR)
                    ) AS CustomerName
                FROM adventureworks_staging.stg_sales_customer c
                LEFT JOIN adventureworks_staging.stg_person_person p ON c.personid = p.businessentityid
                LEFT JOIN adventureworks_staging.stg_sales_store s ON c.storeid = s.businessentityid
                LEFT JOIN adventureworks_staging.stg_person_emailaddress e_name ON c.personid = e_name.businessentityid
            )
            SELECT 
                cb.customerid,
                cb.CustomerName,
                e.emailaddress,
                ph.phonenumber,
                a.city,
                sp.name,
                cr.name,
                a.postalcode,
                -- DYNAMIC SEGMENT LOOKUP (Replacing the CASE statement)
                COALESCE(seg.SegmentName, 'Budget') AS CustomerSegment,
                CASE WHEN cb.storeid IS NOT NULL THEN 'Corporate' ELSE 'Individual' END,
                'Active', 0.00, 0.00,
                COALESCE(TIMESTAMPDIFF(YEAR, stats.FirstPurchaseDate, CURRENT_DATE()), 0),
                CURRENT_DATE()
            FROM CustomerBase cb
            LEFT JOIN CustomerSalesStats stats ON cb.customerid = stats.customerid
            -- The Non-Equi Join:
            LEFT JOIN DimCustomerSegment seg 
                ON stats.TotalLifetimeSpend >= (seg.DiscountTierStart * 10000) -- Example: 0.10 * 10000 = $1000
               AND stats.TotalLifetimeSpend < (seg.DiscountTierEnd * 10000)
            LEFT JOIN adventureworks_staging.stg_person_emailaddress e ON cb.personid = e.businessentityid
            LEFT JOIN adventureworks_staging.stg_person_personphone ph ON cb.personid = ph.businessentityid
            LEFT JOIN adventureworks_staging.stg_person_businessentityaddress bea 
                ON (cb.personid = bea.businessentityid OR cb.storeid = bea.businessentityid)
            LEFT JOIN adventureworks_staging.stg_person_address a ON bea.addressid = a.addressid
            LEFT JOIN adventureworks_staging.stg_person_stateprovince sp ON a.stateprovinceid = sp.stateprovinceid
            LEFT JOIN adventureworks_staging.stg_person_countryregion cr ON sp.countryregioncode = cr.countryregioncode
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. EXPIRE logic
        expire_sql = f"""
            UPDATE DimCustomer
            SET IsCurrent = FALSE, 
                ValidToDate = '{yesterday}', 
                EffectiveEndDate = '{yesterday}'
            FROM adventureworks_staging.stg_dim_customer_upsert s
            WHERE DimCustomer.CustomerID = s.CustomerID
            AND DimCustomer.IsCurrent = TRUE 
            AND (DimCustomer.Email != s.Email OR DimCustomer.City != s.City);
        """
        starrocks_hook.run(expire_sql)

        # 4. INSERT logic
        insert_sql = f"""
            INSERT INTO DimCustomer (
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
            LEFT JOIN DimCustomer d ON s.CustomerID = d.CustomerID AND d.IsCurrent = TRUE
            WHERE d.CustomerID IS NULL;
        """
        starrocks_hook.run(insert_sql)

    load_dim_customer_scd2()

extract_transform_combine_customer_data_into_dimcustomer_and_upload_to_starrocks()