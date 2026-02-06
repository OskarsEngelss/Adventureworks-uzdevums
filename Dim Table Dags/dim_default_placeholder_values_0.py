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
        
        # SQL List for all 15 dimensions
        seed_queries = [
            # 1. DimCustomer (SCD2)
            """INSERT INTO adventureworks.DimCustomer 
               (CustomerKey, CustomerID, CustomerName, Email, City, IsCurrent, ValidFromDate) 
               VALUES (0, 0, 'Unknown Customer', 'unknown@example.com', 'Unknown', TRUE, '1900-01-01');""",
            
            # 2. DimProduct (SCD2)
            """INSERT INTO adventureworks.DimProduct 
               (ProductKey, ProductID, ProductName, SKU, ProductStatus, IsCurrent, ValidFromDate) 
               VALUES (0, 0, 'Unknown Product', 'N/A', 'Active', TRUE, '1900-01-01');""",
               
            # 3. DimPromotion (SCD1)
            """INSERT INTO adventureworks.DimPromotion 
               (PromotionKey, PromotionID, PromotionName, PromotionType, DiscountPercentage) 
               VALUES (0, 0, 'No Promotion', 'N/A', 0.00);""",
            
            # 4. DimEmployee (SCD2)
            """INSERT INTO adventureworks.DimEmployee 
               (EmployeeKey, EmployeeID, EmployeeName, JobTitle, IsCurrent, ValidFromDate) 
               VALUES (0, 0, 'Unknown Employee', 'N/A', TRUE, '1900-01-01');""",

            # 5. DimStore (SCD2)
            """INSERT INTO adventureworks.DimStore 
               (StoreKey, StoreID, StoreName, Region, Territory, StoreStatus, IsCurrent, ValidFromDate) 
               VALUES (0, 0, 'Online/Unknown', 'Online', 'Online', 'N/A', TRUE, '1900-01-01');""",

            # 6. DimVendor (SCD2)
            """INSERT INTO adventureworks.DimVendor 
               (VendorKey, VendorID, VendorName, VendorStatus, IsCurrent, ValidFromDate) 
               VALUES (0, 0, 'Unknown Vendor', 'N/A', TRUE, '1900-01-01');""",

            # 7. DimWarehouse (SCD2)
            """INSERT INTO adventureworks.DimWarehouse 
               (WarehouseKey, WarehouseID, WarehouseName, WarehouseType, IsCurrent, ValidFromDate) 
               VALUES (0, 0, 'Unknown Warehouse', 'N/A', TRUE, '1900-01-01');""",

            # 8. DimRegion (SCD1 - Static)
            """INSERT INTO adventureworks.DimRegion 
               (RegionKey, RegionID, RegionName, Country, Continent, TimeZone) 
               VALUES (0, 0, 'Online', 'Online', 'Online', 'UTC');""",

            # 9. DimSalesTerritory (SCD1)
            """INSERT INTO adventureworks.DimSalesTerritory 
               (TerritoryKey, TerritoryID, TerritoryName, SalesRegion, Country) 
               VALUES (0, 0, 'Online', 'Online', 'Online');""",

            # 10. DimReturnReason (SCD1 - Static)
            """INSERT INTO adventureworks.DimReturnReason 
               (ReturnReasonKey, ReturnReasonID, ReturnReasonName) 
               VALUES (0, 0, 'No Reason Given');""",

            # 11. DimProductCategory (SCD1)
            """INSERT INTO adventureworks.DimProductCategory 
               (ProductCategoryKey, ProductCategoryID, CategoryName) 
               VALUES (0, 0, 'Unknown Category');""",

            # 12. DimCustomerSegment (SCD1 - Static)
            """INSERT INTO adventureworks.DimCustomerSegment 
               (SegmentKey, SegmentID, SegmentName) 
               VALUES (0, 0, 'Uncategorized');""",

            # 13. DimAgingTier (SCD1 - Static)
            """INSERT INTO adventureworks.DimAgingTier 
               (AgingTierKey, AgingTierID, AgingTierName) 
               VALUES (0, 0, 'N/A');""",

            # 14. DimFeedbackCategory (SCD1 - Static)
            """INSERT INTO adventureworks.DimFeedbackCategory 
               (FeedbackCategoryKey, FeedbackCategoryID, CategoryName) 
               VALUES (0, 0, 'General Feedback');""",

            # 15. DimFinanceCategory (SCD1 - Static)
            """INSERT INTO adventureworks.DimFinanceCategory 
               (FinanceCategoryKey, FinanceCategoryID, CategoryName) 
               VALUES (0, 0, 'Unassigned');"""

            # 16. DimDate (Static)
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
               );"""
        ]
        
        for query in seed_queries:
            try:
                starrocks_hook.run(query)
            except Exception as e:
                print(f"Error seeding table: {e}")

    insert_placeholder_values()

seed_dimension_placeholders_0()