import datetime
import pendulum
import pandas as pd
import uuid

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Schemas and keywords to automatically find each table
ADVENTUREWORKS_SCHEMAS = [
    "sales", "person", "production", "purchasing", "humanresources"
]

FACT_KEYWORDS = [
    "order", "detail", "history", "transaction", "workorder",
    "inventory", "purchase", "salesorder"
]

DIM_KEYWORDS = [
    "name", "description", "type", "category", "address",
    "currency", "creditcard", "product", "customer", "vendor"
]


def classify_table(table_name, columns):
    """Simple heuristic to label tables as fact or dimension."""
    name = table_name.lower()
    colnames = [c.lower() for c in columns]

    # Finds fact tables if keywords match
    if any(k in name for k in FACT_KEYWORDS):
        return "fact"

    # Finds fact tables if keywords match
    measure_keywords = ["amount", "qty", "price", "total"]
    has_date = any("date" in c for c in colnames)
    has_measure = any(any(kw in c for kw in measure_keywords) for c in colnames)
    if has_date and has_measure:
        return "fact"

    # If table didnt match fact keywords = dim table
    return "dimension"


@dag(
    dag_id="extract_incremental_data_adventureworks",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 12, 18, tz=pendulum.timezone("Europe/Tallinn")),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def adventureworks_dag():

    @task
    def discover_tables():
        """List all tables + columns + classify them."""
        hook = PostgresHook(postgres_conn_id="postgres_source")
        conn = hook.get_conn()
        cur = conn.cursor()

        # Gets all tables
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name;
        """, (tuple(ADVENTUREWORKS_SCHEMAS),))
        tables = cur.fetchall()

        results = []
        for schema, table in tables:
            # Gets column names
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
            """, (schema, table))
            columns = [row[0] for row in cur.fetchall()]

            # Determines if a table if a fact or dim
            table_type = classify_table(table, columns)

            results.append({
                "schema": schema,
                "table": table,
                "columns": columns,
                "type": table_type,
            })

        return results


    @task
    def extract_incremental_data(tables):
        """Extract data from Postgres based on table type."""
        hook = PostgresHook(postgres_conn_id="postgres_source")
        extracted = {}

        for t in tables:
            schema = t["schema"]
            table = t["table"]
            table_type = t["type"]
            columns = t["columns"]

            df = None  # Always define df

            # Always extract person.* tables
            if schema == "person":
                df = hook.get_pandas_df(sql=f"SELECT * FROM {schema}.{table}")

            # Dimension tables with ModifiedDate
            elif table_type == "dimension":
                if "modifieddate" in [c.lower() for c in columns]:
                    df = hook.get_pandas_df(sql=f"""
                        SELECT *
                        FROM {schema}.{table}
                        WHERE modifieddate > '2000-01-01'
                    """)
                else:
                    continue

            # Fact tables with date columns
            else:
                date_cols = [c for c in columns if "date" in c.lower()]
                if not date_cols:
                    continue
                date_col = date_cols[0]
                df = hook.get_pandas_df(sql=f"""
                    SELECT *
                    FROM {schema}.{table}
                    WHERE {date_col} BETWEEN '2012-01-01' AND '2014-12-31'
                """)

            if df is None:
                continue

            # Normalize object columns
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].astype(str)

            extracted[f"{schema}.{table}"] = {
                "df": df,
                "columns": columns,
                "type": table_type
            }

        return extracted


    @task
    def validate_extracted_data(extracted_data: dict):
        """Run basic data quality checks."""
        hook = PostgresHook(postgres_conn_id="postgres_source")
        conn = hook.get_conn()
        cur = conn.cursor()

        validation_summary = {}

        for table_name, payload in extracted_data.items():
            df = payload["df"]
            columns = payload["columns"]

            table_errors = []
            table_warnings = []

            # Required columns (NOT NULL)
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                  AND is_nullable = 'NO'
            """, table_name.split("."))
            non_nullable_cols = [row[0] for row in cur.fetchall()]

            # Check nulls
            for col in non_nullable_cols:
                if col in df.columns and df[col].isna().sum() > 0:
                    table_errors.append(f"Nulls in {col}")

            # Numeric type check
            numeric_cols = [c for c in df.columns if df[c].dtype.kind in "iuf"]
            for col in numeric_cols:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    table_errors.append(f"Bad numeric type: {col}")

            # Future dates
            date_cols = [c for c in df.columns if "date" in c.lower()]
            for col in date_cols:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    if (df[col] > pd.Timestamp.now()).any():
                        table_warnings.append(f"Future dates in {col}")

            # Negative revenue
            revenue_cols = [c for c in df.columns if any(k in c.lower() for k in ["amount", "price", "total"])]
            for col in revenue_cols:
                if (df[col] < 0).any():
                    table_errors.append(f"Negative values in {col}")

            # Duplicate primary keys
            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = %s
                  AND tc.table_name = %s
                  AND tc.constraint_type = 'PRIMARY KEY'
            """, table_name.split("."))
            pk_cols = [row[0] for row in cur.fetchall()]

            if pk_cols:
                if df.duplicated(subset=pk_cols).any():
                    table_errors.append("Duplicate primary keys")

            validation_summary[table_name] = {
                "status": "PASS" if not table_errors else "FAIL",
                "errors": table_errors,
                "warnings": table_warnings
            }

        return validation_summary




    # Attempt to combine multiple tables into 1 customer table..
    # Not finished!!! Broken for now..

    @task
    def build_customer_staging(extracted_data: dict) -> str:
        """Build Customer staging table and load it into StarRocks."""

        # Load source tables
        person_df = extracted_data.get("person.person", {}).get("df")
        email_df = extracted_data.get("person.emailaddress", {}).get("df")
        phone_df = extracted_data.get("person.personphone", {}).get("df")
        customer_df = extracted_data.get("sales.customer", {}).get("df")
        address_df = extracted_data.get("person.address", {}).get("df")
        beaddr_df = extracted_data.get("person.businessentityaddress", {}).get("df")
        salesorder_df = extracted_data.get("sales.salesorderheader", {}).get("df")

        if person_df is None or customer_df is None:
            raise ValueError("Missing person.person or sales.customer")

        # Clean column names
        for df in [person_df, email_df, phone_df, customer_df, address_df, beaddr_df, salesorder_df]:
            if df is not None:
                df.columns = [c.lower() for c in df.columns]
                df.drop(columns=[c for c in df.columns if c == "modifieddate"], inplace=True, errors="ignore")

        # Join person + email + phone
        person_full = person_df.copy()
        if email_df is not None:
            person_full = person_full.merge(email_df, on="businessentityid", how="left")
        if phone_df is not None:
            person_full = person_full.merge(phone_df, on="businessentityid", how="left")

        person_full["customername"] = (
            person_full["firstname"].fillna("") + " " + person_full["lastname"].fillna("")
        ).str.strip()

        # Join customer
        staging = customer_df.merge(person_full, left_on="personid", right_on="businessentityid", how="left")

        # Join address info
        if beaddr_df is not None and address_df is not None:
            staging = staging.merge(beaddr_df, left_on="personid", right_on="businessentityid", how="left")
            staging = staging.merge(address_df, on="addressid", how="left")

        # Derive fields
        staging["customertype"] = staging["storeid"].apply(lambda x: "Corporate" if pd.notna(x) else "Individual")

        if salesorder_df is not None and "customerid" in salesorder_df.columns:
            fp = salesorder_df.groupby("customerid")["orderdate"].min().reset_index()
            fp.rename(columns={"orderdate": "firstpurchase"}, inplace=True)
            staging = staging.merge(fp, on="customerid", how="left")
            staging["yearssincefirstpurchase"] = (
                pd.Timestamp.now().year - staging["firstpurchase"].dt.year
            ).fillna(0).astype(int)
        else:
            staging["yearssincefirstpurchase"] = 0

        staging["customersegment"] = staging["yearssincefirstpurchase"].apply(
            lambda y: "Premium" if y >= 5 else "Standard"
        )
        staging["accountstatus"] = "Active"
        staging["creditlimit"] = 0
        staging["annualincome"] = 0

        # Select final columns
        final_cols = [
            "customerid", "customername", "emailaddress", "phonenumber", "city",
            "stateprovinceid", "countryregioncode", "postalcode",
            "customersegment", "customertype", "accountstatus",
            "creditlimit", "annualincome", "yearssincefirstpurchase"
        ]
        final_cols = [c for c in final_cols if c in staging.columns]
        staging_final = staging[final_cols].copy()

        # Replace NaN with None
        staging_final = staging_final.applymap(lambda x: None if pd.isna(x) else x)

        # Load into StarRocks
        sr = MySqlHook(mysql_conn_id="starrocks_mysql")
        conn = sr.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS adventureworks.StagingCustomer (
                customerid BIGINT,
                customername VARCHAR(200),
                emailaddress VARCHAR(200),
                phonenumber VARCHAR(50),
                city VARCHAR(100),
                stateprovinceid BIGINT,
                countryregioncode VARCHAR(10),
                postalcode VARCHAR(20),
                customersegment VARCHAR(50),
                customertype VARCHAR(50),
                accountstatus VARCHAR(50),
                creditlimit BIGINT,
                annualincome BIGINT,
                yearssincefirstpurchase BIGINT
            )
            DUPLICATE KEY(customerid)
            DISTRIBUTED BY HASH(customerid)
            PROPERTIES ("replication_num" = "1");
        """)

        cursor.execute("TRUNCATE TABLE adventureworks.StagingCustomer")

        insert_sql = """
            INSERT INTO adventureworks.StagingCustomer (
                customerid, customername, emailaddress, phonenumber, city,
                stateprovinceid, countryregioncode, postalcode, customersegment,
                customertype, accountstatus, creditlimit, annualincome,
                yearssincefirstpurchase
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_sql, staging_final.values.tolist())
        conn.commit()

        return "adventureworks.StagingCustomer"



    # Attempt to check if customer exists in StarRocks
    # Also not finished!!! First need to fix the combining task!!

    @task
    def detect_dimcustomer_changes(staging_table_name: str):

        sr = MySqlHook(mysql_conn_id="starrocks_mysql")

        # Load staging
        staging_df = sr.get_pandas_df(f"SELECT * FROM {staging_table_name}")
        staging_df.columns = [c.lower() for c in staging_df.columns]

        # Load existing DimCustomer (may be empty initially)
        existing_df = sr.get_pandas_df("""
            SELECT *
            FROM adventureworks.DimCustomer
            WHERE IsCurrent = 1
        """)
        if existing_df is None or existing_df.empty:
            existing_df = pd.DataFrame(columns=["customerid"])
        existing_df.columns = [c.lower() for c in existing_df.columns]

        # Merge on natural key
        merged = staging_df.merge(
            existing_df,
            how="left",
            on="customerid",
            suffixes=("_new", "_old")
        )

        # New customers
        new_customers = merged[merged["customerid_old"].isna()].copy()

        # SCD2 change detection
        scd2_cols = [
            "customername",
            "emailaddress",
            "phonenumber",
            "city",
            "postalcode",
            "customersegment",
            "customertype",
            "accountstatus",
        ]

        changed_mask = False
        for col in scd2_cols:
            new_col = f"{col}_new"
            old_col = f"{col}_old"
            if new_col in merged.columns and old_col in merged.columns:
                changed_mask |= (merged[new_col] != merged[old_col])

        changed_customers = merged[
            changed_mask & merged["customerid_old"].notna()
        ].copy()

        unchanged_customers = merged[
            (~changed_mask) & merged["customerid_old"].notna()
        ].copy()

        return {
            "new_customers": new_customers.to_dict(orient="records"),
            "changed_customers": changed_customers.to_dict(orient="records"),
            "unchanged_customers": unchanged_customers.to_dict(orient="records"),
        }




    # Task order - work in progress whilst the other 2 tasks are broken..
    
    tables = discover_tables()
    extracted = extract_incremental_data(tables)
    validated = validate_extracted_data(extracted)

    staging_table = build_customer_staging(extracted)
    dimcustomer_changes = detect_dimcustomer_changes(staging_table)



dag_instance = adventureworks_dag()