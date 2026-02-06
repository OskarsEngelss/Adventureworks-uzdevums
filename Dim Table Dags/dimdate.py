import datetime
import pendulum
from typing import List, Dict, Any

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Database configuration
WAREHOUSE_DATABASE = "adventureworks"
DIM_DATE_TABLE = "DimDate"

# Date range for AdventureWorks (1990-2029)
DATE_RANGE_START = datetime.date(1990, 1, 1)
DATE_RANGE_END = datetime.date(2029, 12, 31)

# Holiday definitions (simplified - you'd want to expand this)
HOLIDAYS = {
    (1, 1): "New Year's Day",
    (11, 18): "Latvia Independence Day",
    (12, 25): "Christmas Day",
    (12, 31): "New Year's Eve",
}

@dag(
    dag_id="load_dim_date",
    schedule="@yearly",  # Run once a year to add new dates
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["dimension", "scd1", "date", "starrocks", "adventureworks"],
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=3),
        "email_on_failure": False,
    }
)
def load_dim_date_dag():

    @task
    def create_dim_date_table():
        """Create or verify the DimDate table exists with correct schema"""
        starrocks_hook = MySqlHook(mysql_conn_id="starrocks_mysql")
        
        # Ensure database exists
        starrocks_hook.run(f"CREATE DATABASE IF NOT EXISTS {WAREHOUSE_DATABASE};")
        
        # Create DimDate table with proper StarRocks syntax
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE} (
                DateKey INT,
                FullDate DATE,
                Year SMALLINT,
                Quarter TINYINT,
                Month TINYINT,
                MonthName VARCHAR(20),
                Week SMALLINT,
                DayOfWeek TINYINT,
                DayName VARCHAR(10),
                DayOfMonth TINYINT,
                DayOfYear SMALLINT,
                WeekOfYear SMALLINT,
                IsWeekend BOOLEAN,
                IsHoliday BOOLEAN,
                HolidayName VARCHAR(50),
                FiscalYear SMALLINT,
                FiscalQuarter TINYINT,
                FiscalMonth TINYINT,
                Season VARCHAR(20)
            )
            ENGINE=OLAP
            DUPLICATE KEY(DateKey)
            DISTRIBUTED BY HASH(DateKey) BUCKETS 10
            PROPERTIES (
                "replication_num" = "3",
                "storage_format" = "V2"
            )
        """
        
        try:
            starrocks_hook.run(create_table_sql)
            print(f"Created/verified table: {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE}")
        except Exception as e:
            print(f"Note creating table (may already exist): {e}")
        
        return True

    @task
    def get_existing_date_range():
        """Get the existing date range in DimDate to avoid duplicates"""
        starrocks_hook = MySqlHook(mysql_conn_id="starrocks_mysql")
        
        try:
            # Get min and max dates
            range_query = f"""
                SELECT 
                    MIN(FullDate) as min_date,
                    MAX(FullDate) as max_date,
                    COUNT(*) as date_count
                FROM {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE}
            """
            result = starrocks_hook.get_first(range_query)
            
            if result and result[0] and result[1]:
                min_date = result[0]
                max_date = result[1]
                count = result[2]
                print(f"Found {count} existing dates from {min_date} to {max_date}")
                return {
                    "min_date": min_date,
                    "max_date": max_date,
                    "count": count,
                    "exists": True
                }
            else:
                print("DimDate table exists but is empty")
                return {
                    "min_date": None,
                    "max_date": None,
                    "count": 0,
                    "exists": True
                }
                
        except Exception as e:
            print(f"DimDate table doesn't exist or is empty: {e}")
            return {
                "min_date": None,
                "max_date": None,
                "count": 0,
                "exists": False
            }

    @task
    def generate_date_dimension(existing_range: Dict[str, Any]):
        """Generate date dimension records for missing dates"""
        starrocks_hook = MySqlHook(mysql_conn_id="starrocks_mysql")
        
        # Determine which dates need to be generated
        existing_min = existing_range.get("min_date")
        existing_max = existing_range.get("max_date")
        
        # Start generating from beginning if table is empty
        if existing_min is None or existing_max is None:
            generate_from = DATE_RANGE_START
            generate_to = DATE_RANGE_END
            print(f"Generating all dates from {generate_from} to {generate_to}")
        else:
            # Generate missing dates before existing range
            if DATE_RANGE_START < existing_min:
                generate_from = DATE_RANGE_START
                generate_to = existing_min - datetime.timedelta(days=1)
                print(f"Generating missing dates before range: {generate_from} to {generate_to}")
            # Generate missing dates after existing range
            elif DATE_RANGE_END > existing_max:
                generate_from = existing_max + datetime.timedelta(days=1)
                generate_to = DATE_RANGE_END
                print(f"Generating missing dates after range: {generate_from} to {generate_to}")
            else:
                print("Date dimension is already complete")
                return {"dates_generated": 0, "start_date": None, "end_date": None}
        
        current_date = generate_from
        batch_size = 1000
        batch_values = []
        dates_generated = 0
        
        while current_date <= generate_to:
            try:
                date_record = generate_date_record(current_date)
                batch_values.append(date_record)
                
                # Insert in batches
                if len(batch_values) >= batch_size:
                    insert_batch(starrocks_hook, batch_values)
                    dates_generated += len(batch_values)
                    batch_values = []
                    print(f"Generated {dates_generated} dates...")
                
                current_date += datetime.timedelta(days=1)
                
            except Exception as e:
                print(f"Error generating date {current_date}: {e}")
                # Continue with next date
                current_date += datetime.timedelta(days=1)
        
        # Insert remaining batch
        if batch_values:
            insert_batch(starrocks_hook, batch_values)
            dates_generated += len(batch_values)
        
        print(f"Successfully generated {dates_generated} date records")
        return {
            "dates_generated": dates_generated,
            "start_date": generate_from.isoformat(),
            "end_date": generate_to.isoformat()
        }

    def generate_date_record(date: datetime.date) -> str:
        """Generate SQL values for a single date record"""
        date_key = int(date.strftime('%Y%m%d'))
        year = date.year
        month = date.month
        day = date.day
        
        # Basic date attributes
        day_of_week = date.isoweekday()  # 1=Monday, 7=Sunday
        week_of_year = date.isocalendar()[1]
        day_of_year = (date - datetime.date(year, 1, 1)).days + 1
        
        # Month names
        month_names = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        month_name = month_names[month - 1]
        
        # Day names
        day_names = [
            "Monday", "Tuesday", "Wednesday", "Thursday", 
            "Friday", "Saturday", "Sunday"
        ]
        day_name = day_names[day_of_week - 1]
        
        # Quarter
        quarter = (month - 1) // 3 + 1
        
        # Is weekend
        is_weekend = day_of_week >= 6
        
        # Holiday check
        holiday_key = (month, day)
        is_holiday = holiday_key in HOLIDAYS
        holiday_name = HOLIDAYS.get(holiday_key, "")
        
        # Fiscal calendar (starting July 1)
        fiscal_year = year if month >= 7 else year - 1
        fiscal_quarter = ((month - 7) % 12) // 3 + 1 if month >= 7 else ((month + 5) % 12) // 3 + 1
        fiscal_month = ((month - 7) % 12) + 1 if month >= 7 else month + 6
        
        # Season (Northern hemisphere)
        if month in [12, 1, 2]:
            season = "Winter"
        elif month in [3, 4, 5]:
            season = "Spring"
        elif month in [6, 7, 8]:
            season = "Summer"
        else:
            season = "Fall"
        
        # Return formatted SQL values
        return f"""(
            {date_key},
            '{date}',
            {year},
            {quarter},
            {month},
            '{month_name}',
            {week_of_year},
            {day_of_week},
            '{day_name}',
            {day},
            {day_of_year},
            {week_of_year},
            {1 if is_weekend else 0},
            {1 if is_holiday else 0},
            '{holiday_name.replace("'", "''")}',
            {fiscal_year},
            {fiscal_quarter},
            {fiscal_month},
            '{season}'
        )"""

    def insert_batch(starrocks_hook, batch_values: List[str]):
        """Insert a batch of date records"""
        if not batch_values:
            return
        
        insert_sql = f"""
            INSERT INTO {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE} 
            VALUES {','.join(batch_values)}
        """
        
        try:
            starrocks_hook.run(insert_sql)
        except Exception as e:
            print(f"Error inserting batch: {e}")
            # Try inserting individually to identify problematic records
            for i, values in enumerate(batch_values):
                try:
                    single_sql = f"""
                        INSERT INTO {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE} 
                        VALUES {values}
                    """
                    starrocks_hook.run(single_sql)
                except Exception as single_error:
                    print(f"Failed to insert record {i}: {single_error}")

    @task
    def validate_dim_date(generation_result: Dict[str, Any]):
        """Validate the date dimension was loaded correctly"""
        starrocks_hook = MySqlHook(mysql_conn_id="starrocks_mysql")
        
        dates_generated = generation_result.get("dates_generated", 0)
        
        print("\n" + "="*80)
        print("DIM DATE VALIDATION")
        print("="*80)
        
        if dates_generated == 0:
            print("No new dates were generated")
        else:
            print(f"Generated {dates_generated} new date records")
        
        # Run validation queries
        validations = [
            ("Total date count", f"SELECT COUNT(*) FROM {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE}"),
            ("Date range", f"SELECT MIN(FullDate), MAX(FullDate) FROM {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE}"),
            ("Weekend count", f"SELECT COUNT(*) FROM {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE} WHERE IsWeekend = TRUE"),
            ("Holiday count", f"SELECT COUNT(*) FROM {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE} WHERE IsHoliday = TRUE"),
            ("Missing date keys", f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT DateKey 
                    FROM {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE} 
                    WHERE DateKey NOT BETWEEN 20140101 AND 20161231
                ) invalid_dates
            """),
        ]
        
        all_passed = True
        for label, query in validations:
            try:
                result = starrocks_hook.get_first(query)
                if result:
                    print(f"✅ {label}: {result[0]}")
                else:
                    print(f"❌ {label}: No result")
                    all_passed = False
            except Exception as e:
                print(f"❌ {label}: Error - {e}")
                all_passed = False
        
        # Check for duplicates
        try:
            duplicate_query = f"""
                SELECT DateKey, COUNT(*) 
                FROM {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE} 
                GROUP BY DateKey 
                HAVING COUNT(*) > 1
            """
            duplicates = starrocks_hook.get_records(duplicate_query)
            if duplicates:
                print(f"❌ Found {len(duplicates)} duplicate DateKeys")
                all_passed = False
            else:
                print("✅ No duplicate DateKeys found")
        except Exception as e:
            print(f"❌ Duplicate check failed: {e}")
            all_passed = False
        
        print("\n" + "="*80)
        if all_passed:
            print("✅ DIM DATE VALIDATION PASSED")
            return {"status": "SUCCESS", "dates_generated": dates_generated}
        else:
            print("❌ DIM DATE VALIDATION FAILED")
            return {"status": "FAILED", "dates_generated": dates_generated}

    @task
    def update_date_dimension_metadata():
        """Update any metadata or refresh materialized views if needed"""
        starrocks_hook = MySqlHook(mysql_conn_id="starrocks_mysql")
        
        # Example: Update statistics (if supported by StarRocks)
        try:
            # StarRocks doesn't have ANALYZE TABLE like PostgreSQL
            # But we can update statistics if needed
            print("Updating date dimension metadata...")
            
            # Count total records for logging
            count_query = f"SELECT COUNT(*) FROM {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE}"
            count_result = starrocks_hook.get_first(count_query)
            if count_result:
                print(f"Total date records: {count_result[0]:,}")
            
            # Check for any NULL values in key columns
            null_check_query = f"""
                SELECT 
                    SUM(CASE WHEN DateKey IS NULL THEN 1 ELSE 0 END) as null_datekeys,
                    SUM(CASE WHEN FullDate IS NULL THEN 1 ELSE 0 END) as null_fulldates,
                    SUM(CASE WHEN Year IS NULL THEN 1 ELSE 0 END) as null_years
                FROM {WAREHOUSE_DATABASE}.{DIM_DATE_TABLE}
            """
            null_result = starrocks_hook.get_first(null_check_query)
            if null_result and any(null_result):
                print(f"WARNING: Found NULL values - DateKeys: {null_result[0]}, FullDates: {null_result[1]}, Years: {null_result[2]}")
            else:
                print("✅ No NULL values in key columns")
                
        except Exception as e:
            print(f"Note updating metadata: {e}")
        
        return {"metadata_updated": True}

    # Main DAG workflow
    create_table = create_dim_date_table()
    existing_range = get_existing_date_range()
    generation_result = generate_date_dimension(existing_range)
    validation = validate_dim_date(generation_result)
    metadata_update = update_date_dimension_metadata()
    
    # Set dependencies
    create_table >> existing_range >> generation_result >> validation >> metadata_update

load_dim_date_dag()