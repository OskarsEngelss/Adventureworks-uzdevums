**DimDate** (SCD Type 1 - Static)
- Attributes:
  - DateKey (INTEGER PRIMARY KEY)
  - FullDate (DATE)
  - Year, Quarter, Month, MonthName, Week, DayOfWeek, DayName
  - DayOfMonth, DayOfYear, WeekOfYear
  - IsWeekend (BOOLEAN)
  - IsHoliday (BOOLEAN)
  - HolidayName (VARCHAR)
  - FiscalYear, FiscalQuarter, FiscalMonth
  - Season (VARCHAR)
- Note: Static reference table, no versioning needed

CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
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
DUPLICATE KEY(DateKey)
DISTRIBUTED BY HASH(DateKey) BUCKETS 10
PROPERTIES("replication_num" = "3");



**DimCustomer** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - CustomerKey (INTEGER PRIMARY KEY - Surrogate Key)
  - CustomerID (INTEGER - Natural Key from source)
  - CustomerName (VARCHAR)
  - Email (VARCHAR)
  - Phone (VARCHAR)
  - City (VARCHAR)
  - StateProvince (VARCHAR)
  - Country (VARCHAR)
  - PostalCode (VARCHAR)
  - CustomerSegment (VARCHAR) - e.g., "Premium", "Standard", "Budget"
  - CustomerType (VARCHAR) - e.g., "Individual", "Corporate"
  - AccountStatus (VARCHAR) - e.g., "Active", "Inactive", "Suspended"
  - CreditLimit (DECIMAL(18,2))
  - AnnualIncome (DECIMAL(18,2))
  - YearsSinceFirstPurchase (INTEGER)
  - **ValidFromDate (DATE)** - When this version became active
  - **ValidToDate (DATE)** - When this version expired (NULL if current)
  - **IsCurrent (BOOLEAN)** - TRUE if current version, FALSE if historical
  - **SourceUpdateDate (DATE)** - When source system was updated
  - **EffectiveStartDate (DATE)** - Effective date in the warehouse
  - **EffectiveEndDate (DATE)** - Effective end date in the warehouse
- Merge Strategy: UPSERT with change detection on (Name, Email, City, Country, Segment, Status)

CREATE TABLE DimCustomer (
    CustomerKey INT,
    CustomerID INT,
    CustomerName VARCHAR(100),
    Email VARCHAR(100),
    Phone VARCHAR(50),
    City VARCHAR(50),
    StateProvince VARCHAR(50),
    Country VARCHAR(50),
    PostalCode VARCHAR(20),
    CustomerSegment VARCHAR(50),
    CustomerType VARCHAR(50),
    AccountStatus VARCHAR(20),
    CreditLimit DECIMAL(18,2),
    AnnualIncome DECIMAL(18,2),
    YearsSinceFirstPurchase INT,
    ValidFromDate DATE,
    ValidToDate DATE NULL,
    IsCurrent BOOLEAN,
    SourceUpdateDate DATE,
    EffectiveStartDate DATE,
    EffectiveEndDate DATE NULL
)
DUPLICATE KEY(CustomerKey)
PARTITION BY RANGE(ValidFromDate) (
    PARTITION p201401 VALUES LESS THAN ('2014-02-01'),
    PARTITION p201402 VALUES LESS THAN ('2014-03-01'),
    PARTITION p201403 VALUES LESS THAN ('2014-04-01'),
    PARTITION p201404 VALUES LESS THAN ('2014-05-01'),
    PARTITION p201405 VALUES LESS THAN ('2014-06-01'),
    PARTITION p201406 VALUES LESS THAN ('2014-07-01'),
    PARTITION p201407 VALUES LESS THAN ('2014-08-01'),
    PARTITION p201408 VALUES LESS THAN ('2014-09-01'),
    PARTITION p201409 VALUES LESS THAN ('2014-10-01'),
    PARTITION p201410 VALUES LESS THAN ('2014-11-01'),
    PARTITION p201411 VALUES LESS THAN ('2014-12-01'),
    PARTITION p201412 VALUES LESS THAN ('2015-01-01')
)
DISTRIBUTED BY HASH(CustomerKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimProduct** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - ProductKey (INTEGER PRIMARY KEY - Surrogate Key)
  - ProductID (INTEGER - Natural Key from source)
  - ProductName (VARCHAR)
  - SKU (VARCHAR)
  - Category (VARCHAR)
  - SubCategory (VARCHAR)
  - Brand (VARCHAR)
  - ListPrice (DECIMAL(18,2))
  - Cost (DECIMAL(18,2))
  - ProductStatus (VARCHAR) - e.g., "Active", "Discontinued", "Coming Soon"
  - Color (VARCHAR)
  - Size (VARCHAR)
  - Weight (DECIMAL(10,3))
  - **ValidFromDate (DATE)** - When this price/category version became active
  - **ValidToDate (DATE)** - When this version expired (NULL if current)
  - **IsCurrent (BOOLEAN)** - TRUE if current version
  - **SourceUpdateDate (DATE)**
  - **EffectiveStartDate (DATE)**
  - **EffectiveEndDate (DATE)**
- Merge Strategy: UPSERT with change detection on (ListPrice, Cost, Category, Status)

CREATE TABLE DimProduct (
    ProductKey INT,
    ProductID INT,
    ProductName VARCHAR(100),
    SKU VARCHAR(50),
    Category VARCHAR(50),
    SubCategory VARCHAR(50),
    Brand VARCHAR(50),
    ListPrice DECIMAL(18,2),
    Cost DECIMAL(18,2),
    ProductStatus VARCHAR(20),
    Color VARCHAR(20),
    Size VARCHAR(20),
    Weight DECIMAL(10,3),
    ValidFromDate DATE,
    ValidToDate DATE NULL,
    IsCurrent BOOLEAN,
    SourceUpdateDate DATE,
    EffectiveStartDate DATE,
    EffectiveEndDate DATE NULL
)
DUPLICATE KEY(ProductKey)
PARTITION BY RANGE(ValidFromDate) (
    PARTITION p201401 VALUES LESS THAN ('2014-02-01'),
    PARTITION p201402 VALUES LESS THAN ('2014-03-01'),
    PARTITION p201403 VALUES LESS THAN ('2014-04-01'),
    PARTITION p201404 VALUES LESS THAN ('2014-05-01'),
    PARTITION p201405 VALUES LESS THAN ('2014-06-01'),
    PARTITION p201406 VALUES LESS THAN ('2014-07-01'),
    PARTITION p201407 VALUES LESS THAN ('2014-08-01'),
    PARTITION p201408 VALUES LESS THAN ('2014-09-01'),
    PARTITION p201409 VALUES LESS THAN ('2014-10-01'),
    PARTITION p201410 VALUES LESS THAN ('2014-11-01'),
    PARTITION p201411 VALUES LESS THAN ('2014-12-01'),
    PARTITION p201412 VALUES LESS THAN ('2015-01-01')
)
DISTRIBUTED BY HASH(ProductKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimStore** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - StoreKey (INTEGER PRIMARY KEY - Surrogate Key)
  - StoreID (INTEGER - Natural Key)
  - StoreName (VARCHAR)
  - StoreNumber (INTEGER)
  - Address (VARCHAR)
  - City (VARCHAR)
  - StateProvince (VARCHAR)
  - Country (VARCHAR)
  - PostalCode (VARCHAR)
  - Region (VARCHAR)
  - Territory (VARCHAR)
  - StoreType (VARCHAR) - e.g., "Retail", "Warehouse", "Outlet"
  - StoreStatus (VARCHAR) - e.g., "Open", "Closed", "Remodeling"
  - ManagerName (VARCHAR)
  - OpeningDate (DATE)
  - SquareFootage (INTEGER)
  - **ValidFromDate (DATE)**
  - **ValidToDate (DATE)**
  - **IsCurrent (BOOLEAN)**
  - **SourceUpdateDate (DATE)**
- Merge Strategy: UPSERT with change detection on (Address, Region, Territory, Manager, Status)

CREATE TABLE DimStore (
    StoreKey INT,
    StoreID INT,
    StoreName VARCHAR(100),
    StoreNumber INT,
    Address VARCHAR(200),
    City VARCHAR(50),
    StateProvince VARCHAR(50),
    Country VARCHAR(50),
    PostalCode VARCHAR(20),
    Region VARCHAR(50),
    Territory VARCHAR(50),
    StoreType VARCHAR(50),
    StoreStatus VARCHAR(20),
    ManagerName VARCHAR(100),
    OpeningDate DATE,
    SquareFootage INT,
    ValidFromDate DATE,
    ValidToDate DATE NULL,
    IsCurrent BOOLEAN,
    SourceUpdateDate DATE
)
DUPLICATE KEY(StoreKey)
PARTITION BY RANGE(ValidFromDate) (
    PARTITION p201401 VALUES LESS THAN ('2014-02-01'),
    PARTITION p201402 VALUES LESS THAN ('2014-03-01'),
    PARTITION p201403 VALUES LESS THAN ('2014-04-01'),
    PARTITION p201404 VALUES LESS THAN ('2014-05-01'),
    PARTITION p201405 VALUES LESS THAN ('2014-06-01'),
    PARTITION p201406 VALUES LESS THAN ('2014-07-01'),
    PARTITION p201407 VALUES LESS THAN ('2014-08-01'),
    PARTITION p201408 VALUES LESS THAN ('2014-09-01'),
    PARTITION p201409 VALUES LESS THAN ('2014-10-01'),
    PARTITION p201410 VALUES LESS THAN ('2014-11-01'),
    PARTITION p201411 VALUES LESS THAN ('2014-12-01'),
    PARTITION p201412 VALUES LESS THAN ('2015-01-01')
)
DISTRIBUTED BY HASH(StoreKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimEmployee** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - EmployeeKey (INTEGER PRIMARY KEY - Surrogate Key)
  - EmployeeID (INTEGER - Natural Key)
  - EmployeeName (VARCHAR)
  - JobTitle (VARCHAR)
  - Department (VARCHAR)
  - ReportingManagerKey (INTEGER - Self-referencing to DimEmployee)
  - HireDate (DATE)
  - EmployeeStatus (VARCHAR) - e.g., "Active", "On Leave", "Terminated"
  - Region (VARCHAR)
  - Territory (VARCHAR)
  - SalesQuota (DECIMAL(18,2))
  - **ValidFromDate (DATE)**
  - **ValidToDate (DATE)**
  - **IsCurrent (BOOLEAN)**
  - **SourceUpdateDate (DATE)**
- Merge Strategy: UPSERT with change detection on (JobTitle, Department, Region, Territory, Quota)

CREATE TABLE DimEmployee (
    EmployeeKey INT,
    EmployeeID INT,
    EmployeeName VARCHAR(100),
    JobTitle VARCHAR(50),
    Department VARCHAR(50),
    ReportingManagerKey INT,
    HireDate DATE,
    EmployeeStatus VARCHAR(20),
    Region VARCHAR(50),
    Territory VARCHAR(50),
    SalesQuota DECIMAL(18,2),
    ValidFromDate DATE,
    ValidToDate DATE NULL,
    IsCurrent BOOLEAN,
    SourceUpdateDate DATE
)
DUPLICATE KEY(EmployeeKey)
PARTITION BY RANGE(ValidFromDate) (
    PARTITION p201401 VALUES LESS THAN ('2014-02-01'),
    PARTITION p201402 VALUES LESS THAN ('2014-03-01'),
    PARTITION p201403 VALUES LESS THAN ('2014-04-01'),
    PARTITION p201404 VALUES LESS THAN ('2014-05-01'),
    PARTITION p201405 VALUES LESS THAN ('2014-06-01'),
    PARTITION p201406 VALUES LESS THAN ('2014-07-01'),
    PARTITION p201407 VALUES LESS THAN ('2014-08-01'),
    PARTITION p201408 VALUES LESS THAN ('2014-09-01'),
    PARTITION p201409 VALUES LESS THAN ('2014-10-01'),
    PARTITION p201410 VALUES LESS THAN ('2014-11-01'),
    PARTITION p201411 VALUES LESS THAN ('2014-12-01'),
    PARTITION p201412 VALUES LESS THAN ('2015-01-01')
)
DISTRIBUTED BY HASH(EmployeeKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimPromotion** (SCD Type 1 or Type 2 - Configurable per business rules)
- Attributes (SCD Type 1 recommendation):
  - PromotionKey (INTEGER PRIMARY KEY)
  - PromotionID (INTEGER - Natural Key)
  - PromotionName (VARCHAR)
  - PromotionDescription (TEXT)
  - PromotionType (VARCHAR) - e.g., "Discount", "BOGO", "Bundle"
  - DiscountPercentage (DECIMAL(5,2))
  - DiscountAmount (DECIMAL(18,2))
  - StartDate (DATE)
  - EndDate (DATE)
  - IsActive (BOOLEAN)
  - PromotionStatus (VARCHAR)
  - CampaignID (INTEGER)
  - TargetProductKey (INTEGER FK to DimProduct, NULLABLE for store-wide promos)
  - TargetCustomerSegment (VARCHAR, NULLABLE)
- If tracking promotion changes (SCD Type 2): Add ValidFromDate, ValidToDate, IsCurrent

CREATE TABLE DimPromotion (
    PromotionKey INT,
    PromotionID INT,
    PromotionName VARCHAR(100),
    PromotionDescription TEXT,
    PromotionType VARCHAR(50),
    DiscountPercentage DECIMAL(5,2),
    DiscountAmount DECIMAL(18,2),
    StartDate DATE,
    EndDate DATE,
    IsActive BOOLEAN,
    PromotionStatus VARCHAR(20),
    CampaignID INT,
    TargetProductKey INT NULL,
    TargetCustomerSegment VARCHAR(50) NULL
)
DUPLICATE KEY(PromotionKey)
DISTRIBUTED BY HASH(PromotionKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimVendor** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - VendorKey (INTEGER PRIMARY KEY - Surrogate Key)
  - VendorID (INTEGER - Natural Key)
  - VendorName (VARCHAR)
  - ContactPerson (VARCHAR)
  - Email (VARCHAR)
  - Phone (VARCHAR)
  - Address (VARCHAR)
  - City (VARCHAR)
  - Country (VARCHAR)
  - VendorRating (DECIMAL(3,2)) - 1.0 to 5.0
  - OnTimeDeliveryRate (DECIMAL(5,2)) - Percentage
  - QualityScore (DECIMAL(5,2))
  - PaymentTerms (VARCHAR)
  - VendorStatus (VARCHAR) - e.g., "Active", "Inactive", "Preferred"
  - **ValidFromDate (DATE)**
  - **ValidToDate (DATE)**
  - **IsCurrent (BOOLEAN)**
  - **SourceUpdateDate (DATE)**
- Merge Strategy: UPSERT with change detection on (Rating, OnTimeDeliveryRate, QualityScore, Status)

CREATE TABLE DimVendor (
    VendorKey INT,
    VendorID INT,
    VendorName VARCHAR(100),
    ContactPerson VARCHAR(100),
    Email VARCHAR(100),
    Phone VARCHAR(50),
    Address VARCHAR(200),
    City VARCHAR(50),
    Country VARCHAR(50),
    VendorRating DECIMAL(3,2),
    OnTimeDeliveryRate DECIMAL(5,2),
    QualityScore DECIMAL(5,2),
    PaymentTerms VARCHAR(50),
    VendorStatus VARCHAR(20),
    ValidFromDate DATE,
    ValidToDate DATE NULL,
    IsCurrent BOOLEAN,
    SourceUpdateDate DATE
)
DUPLICATE KEY(VendorKey)
PARTITION BY RANGE(ValidFromDate) (
    PARTITION p201401 VALUES LESS THAN ('2014-02-01'),
    PARTITION p201402 VALUES LESS THAN ('2014-03-01'),
    PARTITION p201403 VALUES LESS THAN ('2014-04-01'),
    PARTITION p201404 VALUES LESS THAN ('2014-05-01'),
    PARTITION p201405 VALUES LESS THAN ('2014-06-01'),
    PARTITION p201406 VALUES LESS THAN ('2014-07-01'),
    PARTITION p201407 VALUES LESS THAN ('2014-08-01'),
    PARTITION p201408 VALUES LESS THAN ('2014-09-01'),
    PARTITION p201409 VALUES LESS THAN ('2014-10-01'),
    PARTITION p201410 VALUES LESS THAN ('2014-11-01'),
    PARTITION p201411 VALUES LESS THAN ('2014-12-01'),
    PARTITION p201412 VALUES LESS THAN ('2015-01-01')
)
DISTRIBUTED BY HASH(VendorKey) BUCKETS 10
PROPERTIES("replication_num" = "1");







**DimFeedbackCategory** (SCD Type 1 - Static)
- Attributes:
  - FeedbackCategoryKey (INTEGER PRIMARY KEY)
  - FeedbackCategoryID (INTEGER)
  - CategoryName (VARCHAR) - e.g., "Product Quality", "Delivery", "Customer Service", "Price"
  - CategoryDescription (TEXT)

CREATE TABLE DimFeedbackCategory (
    FeedbackCategoryKey INT,
    FeedbackCategoryID INT,
    CategoryName VARCHAR(50),
    CategoryDescription TEXT
)
PRIMARY KEY(FeedbackCategoryKey)
DISTRIBUTED BY HASH(FeedbackCategoryKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimReturnReason** (SCD Type 1 - Static)
- Attributes:
  - ReturnReasonKey (INTEGER PRIMARY KEY)
  - ReturnReasonID (INTEGER)
  - ReturnReasonName (VARCHAR) - e.g., "Defective", "Wrong Item", "Changed Mind", "Damaged"
  - ReturnReasonDescription (TEXT)

CREATE TABLE DimReturnReason (
    ReturnReasonKey INT,
    ReturnReasonID INT,
    ReturnReasonName VARCHAR(50),
    ReturnReasonDescription TEXT
)
PRIMARY KEY(ReturnReasonKey)
DISTRIBUTED BY HASH(ReturnReasonKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimWarehouse** (SCD Type 2 - Optional, if separate from DimStore)
- Attributes:
  - WarehouseKey (INTEGER PRIMARY KEY)
  - WarehouseID (INTEGER)
  - WarehouseName (VARCHAR)
  - Location (VARCHAR)
  - WarehouseType (VARCHAR) - e.g., "Distribution Center", "Regional Hub"
  - ManagerKey (INTEGER FK to DimEmployee)
  - **ValidFromDate (DATE)**
  - **ValidToDate (DATE)**
  - **IsCurrent (BOOLEAN)**

CREATE TABLE DimWarehouse (
    WarehouseKey INT,
    WarehouseID INT,
    WarehouseName VARCHAR(100),
    Location VARCHAR(100),
    WarehouseType VARCHAR(50),
    ManagerKey INT,
    ValidFromDate DATE,
    ValidToDate DATE NULL,
    IsCurrent BOOLEAN
)
DUPLICATE KEY(WarehouseKey)
PARTITION BY RANGE(ValidFromDate) (
    PARTITION p201401 VALUES LESS THAN ('2014-02-01')
)
DISTRIBUTED BY HASH(WarehouseKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimSalesTerritory** (SCD Type 1 or Type 2)
- Attributes:
  - TerritoryKey (INTEGER PRIMARY KEY)
  - TerritoryID (INTEGER)
  - TerritoryName (VARCHAR)
  - SalesRegion (VARCHAR)
  - Country (VARCHAR)
  - Manager (VARCHAR)
  - SalesTarget (DECIMAL(18,2))


CREATE TABLE DimSalesTerritory (
    TerritoryKey INT,
    TerritoryID INT,
    TerritoryName VARCHAR(50),
    SalesRegion VARCHAR(50),
    Country VARCHAR(50),
    Manager VARCHAR(50),
    SalesTarget DECIMAL(18,2)
)
PRIMARY KEY(TerritoryKey)
DISTRIBUTED BY HASH(TerritoryKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimCustomerSegment** (SCD Type 1 - Static)
- Attributes:
  - SegmentKey (INTEGER PRIMARY KEY)
  - SegmentID (INTEGER)
  - SegmentName (VARCHAR) - e.g., "Premium", "Standard", "Budget", "VIP"
  - SegmentDescription (TEXT)
  - DiscountTierStart (DECIMAL(5,2))
  - DiscountTierEnd (DECIMAL(5,2))

CREATE TABLE DimCustomerSegment (
    SegmentKey INT,
    SegmentID INT,
    SegmentName VARCHAR(50),
    SegmentDescription TEXT,
    DiscountTierStart DECIMAL(5,2),
    DiscountTierEnd DECIMAL(5,2)
)
PRIMARY KEY(SegmentKey)
DISTRIBUTED BY HASH(SegmentKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimAgingTier** (SCD Type 1 - Static)
- Attributes:
  - AgingTierKey (INTEGER PRIMARY KEY)
  - AgingTierID (INTEGER)
  - AgingTierName (VARCHAR) - e.g., "Fresh (0-30 days)", "Aged (31-90 days)", "Very Aged (90+ days)"
  - MinAgingDays (INTEGER)
  - MaxAgingDays (INTEGER)

CREATE TABLE DimAgingTier (
    AgingTierKey INT,
    AgingTierID INT,
    AgingTierName VARCHAR(50),
    MinAgingDays INT,
    MaxAgingDays INT
)
PRIMARY KEY(AgingTierKey)
DISTRIBUTED BY HASH(AgingTierKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimFinanceCategory** (SCD Type 1 - Static)
- Attributes:
  - FinanceCategoryKey (INTEGER PRIMARY KEY)
  - FinanceCategoryID (INTEGER)
  - CategoryName (VARCHAR) - e.g., "Invoice", "Payment", "Credit Memo", "Adjustment"
  - CategoryDescription (TEXT)

CREATE TABLE DimFinanceCategory (
    FinanceCategoryKey INT,
    FinanceCategoryID INT,
    CategoryName VARCHAR(50),
    CategoryDescription TEXT
)
PRIMARY KEY(FinanceCategoryKey)
DISTRIBUTED BY HASH(FinanceCategoryKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimRegion** (SCD Type 1 - Static)
- Attributes:
  - RegionKey (INTEGER PRIMARY KEY)
  - RegionID (INTEGER)
  - RegionName (VARCHAR)
  - Country (VARCHAR)
  - Continent (VARCHAR)
  - TimeZone (VARCHAR)

CREATE TABLE DimRegion (
    RegionKey INT,
    RegionID INT,
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    Continent VARCHAR(50),
    TimeZone VARCHAR(50)
)
PRIMARY KEY(RegionKey)
DISTRIBUTED BY HASH(RegionKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



**DimProductCategory** (SCD Type 1 - Static)
- Attributes:
  - ProductCategoryKey (INTEGER PRIMARY KEY)
  - ProductCategoryID (INTEGER)
  - CategoryName (VARCHAR)
  - CategoryDescription (TEXT)

CREATE TABLE DimProductCategory (
    ProductCategoryKey INT,
    ProductCategoryID INT,
    CategoryName VARCHAR(50),
    CategoryDescription TEXT
)
PRIMARY KEY(ProductCategoryKey)
DISTRIBUTED BY HASH(ProductCategoryKey) BUCKETS 10
PROPERTIES("replication_num" = "1");