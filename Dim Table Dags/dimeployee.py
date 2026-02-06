import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utilities import log_error_to_warehouse

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_employee_data_into_dimemployee_and_upload_to_starrocks",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "dimemployee", "load", "adventureworks"],
    default_args={
        "retries": 3,
        "retry_delay": datetime.timedelta(seconds=30),
    }
)
def extract_transform_combine_employee_data_into_dimemployee_and_upload_to_starrocks():
    
    @task
    def synchronize_postgresql_to_starrocks():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        proc_date = pendulum.now().to_date_string()
        yesterday = pendulum.now().subtract(days=1).to_date_string()

        # --- STEP 1: QUARANTINE (Future Hires or Missing IDs) ---
        quarantine_sql = """
            INSERT INTO adventureworks_errors.error_records (
                ErrorDate, SourceTable, RecordNaturalKey, FailureReason, 
                IsRecoverable, RetryCount, IsResolved, FailedData
            )
            SELECT 
                CURRENT_TIMESTAMP(),
                'DimEmployee',
                COALESCE(CAST(stg.businessentityid AS CHAR), 'UNKNOWN'),
                CASE 
                    WHEN stg.hiredate > CURRENT_DATE() THEN 'Future Hire Date'
                    WHEN stg.businessentityid IS NULL THEN 'Missing EmployeeID'
                END,
                1, 0, 0,
                json_object(
                    'employeeid', CAST(stg.businessentityid AS CHAR),
                    'hiredate', CAST(stg.hiredate AS CHAR)
                )
            FROM adventureworks_staging.stg_humanresources_employee stg
            WHERE stg.businessentityid IS NULL OR stg.hiredate > CURRENT_DATE();
        """
        starrocks_hook.run(quarantine_sql)

        try:
            # --- STEP 2: CLEAN & TRANSFORM INTO STAGING ---
            starrocks_hook.run("TRUNCATE TABLE adventureworks_staging.stg_dim_employee_upsert;")

            load_staging_sql = """
                INSERT INTO adventureworks_staging.stg_dim_employee_upsert
                SELECT 
                    data.EmployeeID,
                    data.StoreID,
                    data.EmployeeName,
                    data.jobtitle,
                    data.Department,
                    data.ReportingManagerKey,
                    data.hiredate,
                    data.EmployeeStatus,
                    data.Region,
                    data.Territory,
                    data.SalesQuota,
                    CURRENT_DATE() AS SourceUpdateDate
                FROM (
                    SELECT 
                        e.businessentityid AS EmployeeID,
                        COALESCE(ss_direct.businessentityid, st_map.MainStoreID, 0) AS StoreID,
                        CONCAT(p.firstname, ' ', p.lastname) AS EmployeeName,
                        e.jobtitle,
                        d.name AS Department,
                        CASE 
                            WHEN mgr.businessentityid IS NOT NULL 
                            THEN murmur_hash3_32(CAST(mgr.businessentityid AS CHAR)) 
                            ELSE 0 
                        END AS ReportingManagerKey,
                        e.hiredate,
                        CASE WHEN e.currentflag = 1 THEN 'Active' ELSE 'Terminated' END AS EmployeeStatus,
                        st.`group` AS Region,
                        st.name AS Territory,
                        COALESCE(sq.salesquota, 0) AS SalesQuota,
                        ROW_NUMBER() OVER(PARTITION BY e.businessentityid ORDER BY sq.modifieddate DESC) as q_rank
                    FROM adventureworks_staging.stg_humanresources_employee e
                    INNER JOIN adventureworks_staging.stg_person_person p 
                        ON e.businessentityid = p.businessentityid
                    INNER JOIN adventureworks_staging.stg_humanresources_employeedepartmenthistory edh 
                        ON e.businessentityid = edh.businessentityid AND edh.enddate IS NULL
                    INNER JOIN adventureworks_staging.stg_humanresources_department d 
                        ON edh.departmentid = d.departmentid
                    LEFT JOIN adventureworks_staging.stg_sales_salesperson sp 
                        ON e.businessentityid = sp.businessentityid
                    LEFT JOIN adventureworks_staging.stg_sales_store ss_direct
                        ON sp.businessentityid = ss_direct.salespersonid
                    LEFT JOIN (
                        SELECT 
                            sc.territoryid, 
                            ss.businessentityid AS MainStoreID,
                            ROW_NUMBER() OVER(PARTITION BY sc.territoryid ORDER BY ss.modifieddate DESC) as r
                        FROM adventureworks_staging.stg_sales_store ss
                        JOIN adventureworks_staging.stg_sales_customer sc ON ss.businessentityid = sc.storeid
                    ) st_map ON sp.territoryid = st_map.territoryid AND st_map.r = 1
                    LEFT JOIN adventureworks_staging.stg_sales_salesterritory st 
                        ON sp.territoryid = st.territoryid
                    LEFT JOIN adventureworks_staging.stg_humanresources_employee mgr 
                        ON e.organizationnode LIKE CONCAT(mgr.organizationnode, '%')
                        AND (LENGTH(e.organizationnode) - LENGTH(REPLACE(e.organizationnode, '/', ''))) = 
                            (LENGTH(mgr.organizationnode) - LENGTH(REPLACE(mgr.organizationnode, '/', ''))) + 1
                    LEFT JOIN adventureworks_staging.stg_sales_salespersonquotahistory sq 
                        ON sp.businessentityid = sq.businessentityid
                    WHERE e.hiredate <= CURRENT_DATE()
                ) data
                WHERE data.q_rank = 1;
            """
            starrocks_hook.run(load_staging_sql)

            # --- STEP 3: SCD TYPE 2 EXPIRE ---
            # Fixed: Use {yesterday} variable instead of hardcoded strings
            expire_sql = f"""
                UPDATE adventureworks.DimEmployee
                SET IsCurrent = FALSE, 
                    ValidToDate = '{yesterday}'
                FROM adventureworks_staging.stg_dim_employee_upsert s
                WHERE adventureworks.DimEmployee.EmployeeID = s.EmployeeID
                AND adventureworks.DimEmployee.IsCurrent = TRUE 
                AND (
                    adventureworks.DimEmployee.JobTitle != s.JobTitle OR 
                    adventureworks.DimEmployee.Department != s.Department OR 
                    adventureworks.DimEmployee.Territory != s.Territory OR 
                    adventureworks.DimEmployee.StoreID != s.StoreID
                );
            """
            starrocks_hook.run(expire_sql)

            # --- STEP 4: SCD TYPE 2 INSERT ---
            # Fixed: Use {proc_date} variable and stable key generation logic
            insert_sql = f"""
                INSERT INTO adventureworks.DimEmployee (
                    EmployeeKey, ValidFromDate, EmployeeID, StoreID, EmployeeName, JobTitle, 
                    Department, ReportingManagerKey, HireDate, EmployeeStatus, 
                    Region, Territory, SalesQuota, ValidToDate, IsCurrent, SourceUpdateDate
                )
                SELECT 
                    murmur_hash3_32(CAST(s.EmployeeID AS CHAR)),
                    '{proc_date}', s.EmployeeID, s.StoreID, s.EmployeeName, s.JobTitle,
                    s.Department, s.ReportingManagerKey, s.HireDate, s.EmployeeStatus,
                    s.Region, s.Territory, s.SalesQuota, 
                    CASE WHEN s.RowRank = 1 THEN NULL ELSE s.SourceUpdateDate END, 
                    CASE WHEN s.RowRank = 1 THEN TRUE ELSE FALSE END, 
                    s.SourceUpdateDate
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY EmployeeID ORDER BY SourceUpdateDate DESC, HireDate DESC) as RowRank
                    FROM adventureworks_staging.stg_dim_employee_upsert
                ) s
                LEFT JOIN adventureworks.DimEmployee d ON s.EmployeeID = d.EmployeeID AND d.IsCurrent = TRUE
                WHERE d.EmployeeID IS NULL;
            """
            starrocks_hook.run(insert_sql)

        except Exception as e:
            log_error_to_warehouse(
                hook=starrocks_hook,
                source_table="DimEmployee",
                natural_key="BATCH_" + proc_date,
                error=e,
                failed_data="Batch failure during synchronize_postgresql_to_starrocks"
            )
            raise

    synchronize_postgresql_to_starrocks()

extract_transform_combine_employee_data_into_dimemployee_and_upload_to_starrocks()