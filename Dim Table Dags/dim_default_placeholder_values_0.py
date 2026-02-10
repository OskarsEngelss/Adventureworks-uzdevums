import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook

STARROCKS_CONNECTION_ID = "starrocks_mysql"

@dag(
    dag_id="dim_default_placeholder_values_0",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Tallinn"),
    catchup=False,
    tags=["starrocks", "setup", "adventureworks"],
)
def seed_dimension_placeholders_0():
    
    @task
    def insert_placeholder_values():
        starrocks_hook = MySqlHook(mysql_conn_id=STARROCKS_CONNECTION_ID)
        
        # We define a list of tuples: (Table Name, Key Column Name, Insert Statement)
        # This allows us to loop and perform the Delete-then-Insert pattern safely.
        dimensions = [
            (
                "DimCustomer", "CustomerKey",
                """INSERT INTO adventureworks.DimCustomer 
                   (CustomerKey, CustomerID, CustomerName, Email, City, IsCurrent, ValidFromDate) 
                   VALUES (0, 0, 'Unknown Customer', 'unknown@example.com', 'Unknown', TRUE, '1900-01-01')"""
            ),
            (
                "DimProduct", "ProductKey",
                """INSERT INTO adventureworks.DimProduct 
                   (ProductKey, ProductID, ProductName, SKU, ProductStatus, IsCurrent, ValidFromDate) 
                   VALUES (0, 0, 'Unknown Product', 'N/A', 'Active', TRUE, '1900-01-01')"""
            ),
            (
                "DimPromotion", "PromotionKey",
                """INSERT INTO adventureworks.DimPromotion 
                   (PromotionKey, PromotionID, PromotionName, PromotionType, DiscountPercentage) 
                   VALUES (0, 0, 'No Promotion', 'N/A', 0.00)"""
            ),
            (
                "DimEmployee", "EmployeeKey",
                """INSERT INTO adventureworks.DimEmployee 
                   (EmployeeKey, EmployeeID, EmployeeName, Department, Region, Territory, JobTitle, IsCurrent, ValidFromDate) 
                   VALUES (0, 0, 'Unknown Employee', 'Unknown Department', 'Unknown Region', 'Unknown Territory', 'Unknown Job Title', TRUE, '1900-01-01')"""
            ),
            (
                "DimStore", "StoreKey",
                """INSERT INTO adventureworks.DimStore 
                   (StoreKey, StoreID, StoreName, Address, City, Country, Region, StoreType, Territory, StoreStatus, IsCurrent, ValidFromDate) 
                   VALUES (0, 0, 'Online', 'Online', 'Online', 'Online', 'Online', 'Online', 'Online', 'N/A', TRUE, '1900-01-01')"""
            ),
            (
                "DimVendor", "VendorKey",
                """INSERT INTO adventureworks.DimVendor 
                   (VendorKey, VendorID, VendorName, VendorStatus, IsCurrent, ValidFromDate) 
                   VALUES (0, 0, 'Unknown Vendor', 'N/A', TRUE, '1900-01-01')"""
            ),
            (
                "DimWarehouse", "WarehouseKey",
                """INSERT INTO adventureworks.DimWarehouse 
                   (WarehouseKey, WarehouseID, WarehouseName, WarehouseType, IsCurrent, ValidFromDate) 
                   VALUES (0, 0, 'Unknown Warehouse', 'N/A', TRUE, '1900-01-01')"""
            ),
            (
                "DimRegion", "RegionKey",
                """INSERT INTO adventureworks.DimRegion 
                   (RegionKey, RegionID, RegionName, Country, Continent, TimeZone) 
                   VALUES (0, 0, 'Online', 'Online', 'Online', 'UTC')"""
            ),
            (
                "DimSalesTerritory", "TerritoryKey",
                """INSERT INTO adventureworks.DimSalesTerritory 
                   (TerritoryKey, TerritoryID, TerritoryName, SalesRegion, Country) 
                   VALUES (0, 0, 'Online', 'Online', 'Online')"""
            ),
            (
                "DimReturnReason", "ReturnReasonKey",
                """INSERT INTO adventureworks.DimReturnReason 
                   (ReturnReasonKey, ReturnReasonID, ReturnReasonName) 
                   VALUES (0, 0, 'No Reason Given')"""
            ),
            (
                "DimProductCategory", "ProductCategoryKey",
                """INSERT INTO adventureworks.DimProductCategory 
                   (ProductCategoryKey, ProductCategoryID, CategoryName) 
                   VALUES (0, 0, 'Unknown Category')"""
            ),
            (
                "DimCustomerSegment", "SegmentKey",
                """INSERT INTO adventureworks.DimCustomerSegment 
                   (SegmentKey, SegmentID, SegmentName) 
                   VALUES (0, 0, 'Uncategorized')"""
            ),
            (
                "DimFeedbackCategory", "FeedbackCategoryKey",
                """INSERT INTO adventureworks.DimFeedbackCategory 
                   (FeedbackCategoryKey, FeedbackCategoryID, CategoryName) 
                   VALUES (0, 0, 'General Feedback')"""
            ),
            (
                "DimFinanceCategory", "FinanceCategoryKey",
                """INSERT INTO adventureworks.DimFinanceCategory 
                   (FinanceCategoryKey, FinanceCategoryID, CategoryName) 
                   VALUES (0, 0, 'Unassigned')"""
            ),
            (
                "DimDate", "DateKey",
                """INSERT INTO adventureworks.DimDate 
                   (
                      DateKey, FullDate, Year, Quarter, Month, MonthName, 
                      Week, DayOfWeek, DayName, DayOfMonth, DayOfYear, 
                      WeekOfYear, IsWeekend, IsHoliday, HolidayName, 
                      FiscalYear, FiscalQuarter, FiscalMonth, Season
                   ) 
                   VALUES 
                   (
                      0, '1900-01-01', 1900, 0, 0, 'Unknown', 
                      0, 0, 'Unknown', 0, 0, 
                      0, FALSE, FALSE, 'N/A', 
                      1900, 0, 0, 'Unknown'
                   )"""
            )
        ]

        for table, key, insert_sql in dimensions:
            try:
                # First, clean up any existing Key=0 (including potential duplicates)
                delete_sql = f"DELETE FROM adventureworks.{table} WHERE {key} = 0;"
                starrocks_hook.run(delete_sql)
                
                # Now, perform a fresh insert
                starrocks_hook.run(insert_sql)
                print(f"Successfully seeded {table}")
                
            except Exception as e:
                print(f"Error seeding {table}: {e}")

    insert_placeholder_values()

seed_dimension_placeholders_0()