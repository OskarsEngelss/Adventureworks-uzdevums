import datetime
import pendulum
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="extract_transform_combine_employee_data_into_dimemployee_and_upload_to_starrocks",
    schedule="@daily",
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

        # 1. Clean Staging
        starrocks_hook.run("DELETE FROM adventureworks_staging.stg_dim_employee_upsert WHERE EmployeeID IS NOT NULL;")

        # 2. TRANSFORM & LOAD TO STAGING
        # Combining HR (Employee, Dept), Person (Names), and Sales (Quota, Territory)
        # 2. TRANSFORM & LOAD TO STAGING
        # 2. TRANSFORM & LOAD TO STAGING
        load_staging_sql = f"""
            INSERT INTO adventureworks_staging.stg_dim_employee_upsert
            SELECT 
                data.EmployeeID,
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
                    CONCAT(p.firstname, ' ', p.lastname) AS EmployeeName,
                    e.jobtitle,
                    d.name AS Department,
                    COALESCE(mgr.businessentityid, 0) AS ReportingManagerKey, 
                    e.hiredate,
                    CASE WHEN e.currentflag = 1 THEN 'Active' ELSE 'Terminated' END AS EmployeeStatus,
                    st.`group` AS Region,
                    st.name AS Territory,
                    COALESCE(sq.salesquota, 0) AS SalesQuota,
                    -- Rank quotas by date so we only pick the latest one
                    ROW_NUMBER() OVER(PARTITION BY e.businessentityid ORDER BY sq.modifieddate DESC) as q_rank
                FROM adventureworks_staging.stg_humanresources_employee e
                INNER JOIN adventureworks_staging.stg_person_person p 
                    ON e.businessentityid = p.businessentityid
                INNER JOIN adventureworks_staging.stg_humanresources_employeedepartmenthistory edh 
                    ON e.businessentityid = edh.businessentityid AND edh.enddate IS NULL
                INNER JOIN adventureworks_staging.stg_humanresources_department d 
                    ON edh.departmentid = d.departmentid
                LEFT JOIN adventureworks_staging.stg_humanresources_employee mgr 
                    ON e.organizationnode LIKE CONCAT(mgr.organizationnode, '%')
                    AND (LENGTH(e.organizationnode) - LENGTH(REPLACE(e.organizationnode, '/', ''))) = 
                        (LENGTH(mgr.organizationnode) - LENGTH(REPLACE(mgr.organizationnode, '/', ''))) + 1
                LEFT JOIN adventureworks_staging.stg_sales_salesperson sp 
                    ON e.businessentityid = sp.businessentityid
                LEFT JOIN adventureworks_staging.stg_sales_salesterritory st 
                    ON sp.territoryid = st.territoryid
                LEFT JOIN adventureworks_staging.stg_sales_salespersonquotahistory sq 
                    ON sp.businessentityid = sq.businessentityid
            ) data
            WHERE data.q_rank = 1;
        """
        starrocks_hook.run(load_staging_sql)

        # 3. EXPIRE logic (Change detection on JobTitle, Department, Region, Territory, Quota)
        expire_sql = f"""
            UPDATE DimEmployee
            SET IsCurrent = FALSE, 
                ValidToDate = '{yesterday}'
            FROM adventureworks_staging.stg_dim_employee_upsert s
            WHERE DimEmployee.EmployeeID = s.EmployeeID
            AND DimEmployee.IsCurrent = TRUE 
            AND (
                DimEmployee.JobTitle != s.JobTitle OR 
                DimEmployee.Department != s.Department OR 
                DimEmployee.Region != s.Region OR 
                DimEmployee.Territory != s.Territory OR 
                DimEmployee.SalesQuota != s.SalesQuota
            );
        """
        starrocks_hook.run(expire_sql)

        # 4. INSERT logic
        insert_sql = f"""
            INSERT INTO DimEmployee (
                EmployeeKey, ValidFromDate, EmployeeID, EmployeeName, JobTitle, 
                Department, ReportingManagerKey, HireDate, EmployeeStatus, 
                Region, Territory, SalesQuota, ValidToDate, IsCurrent, SourceUpdateDate
            )
            SELECT 
                murmur_hash3_32(CONCAT(CAST(s.EmployeeID AS CHAR), '{proc_date}')),
                '{proc_date}', s.EmployeeID, s.EmployeeName, s.JobTitle,
                s.Department, s.ReportingManagerKey, s.HireDate, s.EmployeeStatus,
                s.Region, s.Territory, s.SalesQuota, NULL, TRUE, s.SourceUpdateDate
            FROM adventureworks_staging.stg_dim_employee_upsert s
            LEFT JOIN DimEmployee d ON s.EmployeeID = d.EmployeeID AND d.IsCurrent = TRUE
            WHERE d.EmployeeID IS NULL;
        """
        starrocks_hook.run(insert_sql)

    synchronize_postgresql_to_starrocks()

extract_transform_combine_employee_data_into_dimemployee_and_upload_to_starrocks()