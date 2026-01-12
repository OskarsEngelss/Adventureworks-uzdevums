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
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
            """, (schema, table))
            columns = [row[0] for row in cur.fetchall()]

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
        """Extract data from Postgres source and write staging tables into the SAME Postgres DB."""
        pg = PostgresHook(postgres_conn_id="postgres_source")
        engine = pg.get_sqlalchemy_engine()

        extracted = {}

        for t in tables:
            schema = t["schema"]
            table = t["table"]
            table_type = t["type"]
            columns = t["columns"]

            df = None

            if schema == "person":
                df = pg.get_pandas_df(sql=f"SELECT * FROM {schema}.{table}")

            elif table_type == "dimension":
                if "modifieddate" in [c.lower() for c in columns]:
                    df = pg.get_pandas_df(sql=f"""
                        SELECT *
                        FROM {schema}.{table}
                        WHERE modifieddate > '2000-01-01'
                    """)
                else:
                    continue

            else:
                date_cols = [c for c in columns if "date" in c.lower()]
                if not date_cols:
                    continue
                date_col = date_cols[0]
                df = pg.get_pandas_df(sql=f"""
                    SELECT *
                    FROM {schema}.{table}
                    WHERE {date_col} BETWEEN '2012-01-01' AND '2014-12-31'
                """)

            if df is None:
                continue

            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].astype(str)

            staging_table = f"staging_{schema}_{table}"

            df.to_sql(
                staging_table,
                engine,
                if_exists="replace",
                index=False
            )

            extracted[f"{schema}.{table}"] = {
                "staging_table": staging_table,
                "columns": columns,
                "type": table_type
            }

            print(f"\n=== Loaded {schema}.{table} into {staging_table} ===")
            print(df.head(5).to_string())
            print(f"Rows loaded: {len(df)}\n")

        return extracted







    @task
    def validate_extracted_data(extracted_data: dict):
        """Validate data stored in Postgres staging tables."""
        pg = PostgresHook(postgres_conn_id="postgres_source")
        conn = pg.get_conn()
        cur = conn.cursor()

        validation_summary = {}

        for source_table, payload in extracted_data.items():
            staging_table = payload["staging_table"]
            columns = payload["columns"]

            df = pg.get_pandas_df(sql=f"SELECT * FROM {staging_table}")

            table_errors = []
            table_warnings = []

            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                AND is_nullable = 'NO'
            """, (staging_table,))
            non_nullable_cols = [row[0] for row in cur.fetchall()]

            for col in non_nullable_cols:
                if col in df.columns and df[col].isna().sum() > 0:
                    table_errors.append(f"Nulls in {col}")

            numeric_cols = [c for c in df.columns if df[c].dtype.kind in "iuf"]
            for col in numeric_cols:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    table_errors.append(f"Bad numeric type: {col}")

            date_cols = [c for c in df.columns if "date" in c.lower()]
            for col in date_cols:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    if (df[col] > pd.Timestamp.now()).any():
                        table_warnings.append(f"Future dates in {col}")

            revenue_cols = [
                c for c in df.columns
                if any(k in c.lower() for k in ["amount", "price", "total"])
            ]
            for col in revenue_cols:
                if (df[col] < 0).any():
                    table_errors.append(f"Negative values in {col}")

            schema, table = source_table.split(".")

            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = %s
                AND tc.table_name = %s
                AND tc.constraint_type = 'PRIMARY KEY'
            """, (schema, table))
            pk_cols = [row[0] for row in cur.fetchall()]

            if pk_cols:
                if df.duplicated(subset=pk_cols).any():
                    table_errors.append("Duplicate primary keys")

            validation_summary[source_table] = {
                "staging_table": staging_table,
                "status": "PASS" if not table_errors else "FAIL",
                "errors": table_errors,
                "warnings": table_warnings
            }

        return validation_summary


    @task
    def build_dim_customer():
        import pandas as pd
        from datetime import date

        pg = PostgresHook(postgres_conn_id="postgres_source")

        customer = pg.get_pandas_df("""
            SELECT
                customerid,
                personid,
                storeid,
                territoryid,
                modifieddate AS sourceupdatedate
            FROM staging_sales_customer
        """)

        store = pg.get_pandas_df("""
            SELECT
                businessentityid AS storeid,
                name AS store_name,
                demographics
            FROM staging_sales_store
        """)

        person = pg.get_pandas_df("""
            SELECT
                businessentityid AS personid,
                firstname,
                lastname
            FROM staging_person_person
        """)

        email = pg.get_pandas_df("""
            SELECT
                businessentityid AS personid,
                emailaddress
            FROM staging_person_emailaddress
        """)

        phone = pg.get_pandas_df("""
            SELECT
                businessentityid AS personid,
                phonenumber
            FROM staging_person_personphone
        """)

        address = pg.get_pandas_df("""
            SELECT
                bea.businessentityid,
                a.city,
                a.postalcode,
                sp.name AS state_province,
                cr.name AS country
            FROM staging_person_businessentityaddress bea
            JOIN staging_person_address a
                ON bea.addressid = a.addressid
            JOIN staging_person_stateprovince sp
                ON a.stateprovinceid = sp.stateprovinceid
            JOIN staging_person_countryregion cr
                ON sp.countryregioncode = cr.countryregioncode
        """)

        orders = pg.get_pandas_df("""
            SELECT
                customerid,
                MIN(orderdate) AS first_order_date,
                MAX(orderdate) AS last_order_date,
                SUM(totaldue) AS total_spend
            FROM staging_sales_salesorderheader
            GROUP BY customerid
        """)

        df = (
            customer
            .merge(store, on="storeid", how="left")
            .merge(person, on="personid", how="left")
            .merge(email, on="personid", how="left")
            .merge(phone, on="personid", how="left")
            .merge(address, left_on="storeid", right_on="businessentityid", how="left")
            .merge(address, left_on="personid", right_on="businessentityid", how="left", suffixes=("", "_person"))
            .merge(orders, on="customerid", how="left")
        )


        df["CustomerType"] = df.apply(
            lambda r: "Individual" if pd.notnull(r["personid"]) else "Store",
            axis=1
        )

        df["CustomerName"] = df.apply(
            lambda r: f"{r['firstname']} {r['lastname']}" if r["CustomerType"] == "Individual" else r["store_name"],
            axis=1
        )

        df["Email"] = df["emailaddress"]

        df["Phone"] = df["phonenumber"]

        df["City"] = df.apply(
            lambda r: r["city"] if r["CustomerType"] == "Store" else r["city_person"],
            axis=1
        )
        df["StateProvince"] = df.apply(
            lambda r: r["state_province"] if r["CustomerType"] == "Store" else r["state_province_person"],
            axis=1
        )
        df["Country"] = df.apply(
            lambda r: r["country"] if r["CustomerType"] == "Store" else r["country_person"],
            axis=1
        )
        df["PostalCode"] = df.apply(
            lambda r: r["postalcode"] if r["CustomerType"] == "Store" else r["postalcode_person"],
            axis=1
        )

        df = df.drop(columns=[
            "city",
            "state_province",
            "country",
            "postalcode",
            "city_person",
            "state_province_person",
            "country_person",
            "postalcode_person",
            "businessentityid",
            "businessentityid_person"
        ], errors="ignore")


        def extract_annual_revenue(xml):
            if xml is None:
                return None
            try:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(xml)
                rev = root.find(".//{http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey}AnnualRevenue")
                return float(rev.text) if rev is not None else None
            except:
                return None

        df["AnnualIncome"] = df["demographics"].apply(extract_annual_revenue)

        today = pd.to_datetime(date.today())
        df["YearsSinceFirstPurchase"] = (
            (today - pd.to_datetime(df["first_order_date"])).dt.days // 365
        )

        df["AccountStatus"] = df["last_order_date"].apply(
            lambda d: "Active" if pd.notnull(d) and (today - pd.to_datetime(d)).days <= 365
            else ("Inactive" if pd.notnull(d) else "Prospect")
        )

        def segment(row):
            if row["CustomerType"] == "Store":
                if row["AnnualIncome"] and row["AnnualIncome"] > 500000:
                    return "Enterprise"
                if row["AnnualIncome"] and row["AnnualIncome"] > 100000:
                    return "Mid-Market"
                return "Small Business"
            else:
                if row["total_spend"] and row["total_spend"] > 5000:
                    return "High Value"
                if row["total_spend"] and row["total_spend"] > 500:
                    return "Medium Value"
                return "Low Value"

        df["CustomerSegment"] = df.apply(segment, axis=1)

        df["CreditLimit"] = df.apply(
            lambda r: r["AnnualIncome"] * 0.2 if r["CustomerType"] == "Store"
            else (r["total_spend"] * 1.5 if pd.notnull(r["total_spend"]) else None),
            axis=1
        )

        df["ValidFromDate"] = today
        df["ValidToDate"] = None
        df["IsCurrent"] = True
        df["EffectiveStartDate"] = today
        df["EffectiveEndDate"] = None


        df = df.rename(columns={
            "customerid": "CustomerID",
            "personid": "PersonID",
            "storeid": "StoreID",
            "sourceupdatedate": "SourceUpdateDate",
            "customername": "CustomerName",
            "email": "Email",
            "phone": "Phone",
            "city": "City",
            "state_province": "StateProvince",
            "country": "Country",
            "postalcode": "PostalCode",
            "customersegment": "CustomerSegment",
            "customertype": "CustomerType",
            "accountstatus": "AccountStatus",
            "creditlimit": "CreditLimit",
            "annualincome": "AnnualIncome",
            "yearssincefirstpurchase": "YearsSinceFirstPurchase",
            "validfromdate": "ValidFromDate",
            "validtodate": "ValidToDate",
            "iscurrent": "IsCurrent",
            "effectivestartdate": "EffectiveStartDate",
            "effectiveenddate": "EffectiveEndDate"
        })



        return df




        
    @task
    def transform_dim_customer_for_starrocks(dim_df: pd.DataFrame):
        """Produce a DataFrame that matches the StarRocks DimCustomer schema exactly."""

        df = dim_df.copy()

        required_cols = [
            "CustomerID",
            "CustomerName",
            "Email",
            "Phone",
            "City",
            "StateProvince",
            "Country",
            "PostalCode",
            "CustomerSegment",
            "CustomerType",
            "AccountStatus",
            "CreditLimit",
            "AnnualIncome",
            "YearsSinceFirstPurchase",
            "ValidFromDate",
            "ValidToDate",
            "IsCurrent",
            "SourceUpdateDate",
            "EffectiveStartDate",
            "EffectiveEndDate"
        ]

        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        df["CustomerID"] = df["CustomerID"].astype(int)
        df["YearsSinceFirstPurchase"] = df["YearsSinceFirstPurchase"].fillna(0).astype(int)

        string_cols = [
            "CustomerName", "Email", "Phone", "City", "StateProvince",
            "Country", "PostalCode", "CustomerSegment", "CustomerType",
            "AccountStatus"
        ]
        for col in string_cols:
            df[col] = df[col].fillna("").astype(str)

        df["CreditLimit"] = df["CreditLimit"].fillna(0).astype(float)
        df["AnnualIncome"] = df["AnnualIncome"].fillna(0).astype(float)

        date_cols = [
            "ValidFromDate", "ValidToDate", "SourceUpdateDate",
            "EffectiveStartDate", "EffectiveEndDate"
        ]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        df["IsCurrent"] = df["IsCurrent"].fillna(True).astype(bool)

        df = df[required_cols]

        print("\n=== FINAL STARROCKS DIMCUSTOMER SCHEMA ===")
        print(df.head(10).to_string())
        print(df.dtypes)

        return df



    @task
    def load_dim_customer_scd2(dim_customer_clean: pd.DataFrame):
        import pandas as pd
        from datetime import date, timedelta
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        sr = MySqlHook(mysql_conn_id="starrocks_mysql")

        today = date.today()
        yesterday = today - timedelta(days=1)

        customer_ids = dim_customer_clean["CustomerID"].unique().tolist()

        chunks = [
            customer_ids[i:i + 5000]
            for i in range(0, len(customer_ids), 5000)
        ]

        dfs = []
        for chunk in chunks:
            values_clause = ",".join(f"({int(i)})" for i in chunk)

            sql = f"""
                SELECT d.*
                FROM DimCustomer d
                JOIN (
                    VALUES {values_clause}
                ) AS v(CustomerID)
                ON d.CustomerID = v.CustomerID
                WHERE d.IsCurrent = 1;
            """

            df = sr.get_pandas_df(sql)

            print("=== EXISTING_DF COLUMNS FROM STARROCKS ===")
            print(df.columns.tolist())

            dfs.append(df)

        existing_df = pd.concat(dfs) if dfs else pd.DataFrame()

        merged = dim_customer_clean.merge(
            existing_df,
            on="CustomerID",
            how="left",
            suffixes=("_new", "_old")
        )

        if "CustomerKey" not in merged.columns:
            merged["CustomerKey"] = None

        new_customers = merged[merged["CustomerKey"].isna()].copy()

        compare_cols = [
            "Email",
            "City",
            "Country",
            "CustomerSegment",
            "AccountStatus"
        ]

        def has_changes(row):
            for col in compare_cols:
                if row[f"{col}_new"] != row[f"{col}_old"]:
                    return True
            return False

        changed = merged[
            (~merged["CustomerKey"].isna()) &
            (merged.apply(has_changes, axis=1))
        ].copy()

        updates = []
        for _, row in changed.iterrows():
            updates.append(f"""
                UPDATE DimCustomer
                SET IsCurrent = 0,
                    ValidToDate = '{yesterday}'
                WHERE CustomerKey = {int(row['CustomerKey'])};
            """)

        for sql in updates:
            sr.run(sql)

        new_cols = ["CustomerID"] + [
            f"{col}_new"
            for col in dim_customer_clean.columns
            if col != "CustomerID"
        ]

        inserts = pd.concat([
            new_customers[new_cols],
            changed[new_cols]
        ])

        rename_map = {
            f"{col}_new": col
            for col in dim_customer_clean.columns
            if col != "CustomerID"
        }
        inserts = inserts.rename(columns=rename_map)

        max_key = sr.get_first(
            "SELECT COALESCE(MAX(CustomerKey), 0) FROM DimCustomer;"
        )[0]

        inserts["CustomerKey"] = range(
            max_key + 1,
            max_key + 1 + len(inserts)
        )

        date_cols = [
            "ValidFromDate",
            "ValidToDate",
            "EffectiveStartDate",
            "EffectiveEndDate",
            "SourceUpdateDate"
        ]

        for col in date_cols:
            inserts[col] = (
                inserts[col]
                .astype("object")
                .where(~inserts[col].isna(), None)
            )

        inserts["IsCurrent"] = inserts["IsCurrent"].astype(int)

        if not inserts.empty:
            sr.insert_rows(
                table="DimCustomer",
                rows=inserts.values.tolist(),
                target_fields=inserts.columns.tolist()
            )

        print(f"Inserted {len(inserts)} rows")
        print(f"Updated {len(updates)} rows")

        return True



    tables = discover_tables()
    extracted = extract_incremental_data(tables)
    validated = validate_extracted_data(extracted)

    dim_customer_df = build_dim_customer()
    dim_customer_clean = transform_dim_customer_for_starrocks(dim_customer_df)
    load_task = load_dim_customer_scd2(dim_customer_clean)

    validated >> dim_customer_df
    dim_customer_df >> dim_customer_clean
    dim_customer_clean >> load_task


dag_instance = adventureworks_dag()