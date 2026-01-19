CREATE TABLE adventureworks_staging.stg_dim_customer_upsert (
    CustomerID INT, CustomerName VARCHAR(200), Email VARCHAR(150), Phone VARCHAR(50),
    City VARCHAR(100), StateProvince VARCHAR(100), Country VARCHAR(100), PostalCode VARCHAR(20),
    CustomerSegment VARCHAR(50), CustomerType VARCHAR(50), AccountStatus VARCHAR(20),
    CreditLimit DECIMAL(18,2), AnnualIncome DECIMAL(18,2), YearsSinceFirstPurchase INT,
    SourceUpdateDate DATE
) ENGINE=OLAP DUPLICATE KEY(CustomerID) DISTRIBUTED BY HASH(CustomerID) BUCKETS 10;



-- Old version without StoreID
CREATE TABLE adventureworks_staging.stg_dim_employee_upsert (
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
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(EmployeeID)
DISTRIBUTED BY HASH(EmployeeID) BUCKETS 5;

-- New version with StoreID
CREATE TABLE adventureworks_staging.stg_dim_employee_upsert (
    EmployeeID INT,
    StoreID INT,
    EmployeeName VARCHAR(100),
    JobTitle VARCHAR(50),
    Department VARCHAR(50),
    ReportingManagerKey INT,
    HireDate DATE,
    EmployeeStatus VARCHAR(20),
    Region VARCHAR(50),
    Territory VARCHAR(50),
    SalesQuota DECIMAL(18,2),
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(EmployeeID)
DISTRIBUTED BY HASH(EmployeeID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_store_upsert (
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
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(StoreID)
DISTRIBUTED BY HASH(StoreID) BUCKETS 5;



-- Old version without ReorderPoint and SafetyStockLevel
CREATE TABLE adventureworks_staging.stg_dim_product_upsert (
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
    SourceUpdateDate DATE
)
ENGINE=OLAP
DUPLICATE KEY(ProductID)
DISTRIBUTED BY HASH(ProductID) BUCKETS 10
PROPERTIES("replication_num" = "1");

-- New version with ReorderPoint and SafetyStockLevel
CREATE TABLE adventureworks_staging.stg_dim_product_upsert (
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
    ReorderPoint INT,
    SafetyStockLevel INT,
    SourceUpdateDate DATE
)
ENGINE=OLAP
DUPLICATE KEY(ProductID)
DISTRIBUTED BY HASH(ProductID) BUCKETS 10
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_staging.stg_dim_promotion_upsert (
    PromotionID INT,
    PromotionName VARCHAR(255),
    PromotionDescription VARCHAR(500),
    PromotionType VARCHAR(50),
    DiscountPercentage DECIMAL(5,2),
    DiscountAmount DECIMAL(18,2),
    StartDate DATE,
    EndDate DATE,
    IsActive BOOLEAN,
    PromotionStatus VARCHAR(20),
    CampaignID INT,
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(PromotionID)
DISTRIBUTED BY HASH(PromotionID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_vendor_upsert (
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
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(VendorID)
DISTRIBUTED BY HASH(VendorID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_feedback_category_upsert (
    FeedbackCategoryID INT,
    CategoryName VARCHAR(50),
    CategoryDescription TEXT,
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(FeedbackCategoryID)
DISTRIBUTED BY HASH(FeedbackCategoryID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_return_reason_upsert (
    ReturnReasonID INT,
    ReturnReasonName VARCHAR(50),
    ReturnReasonDescription TEXT,
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(ReturnReasonID)
DISTRIBUTED BY HASH(ReturnReasonID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_warehouse_upsert (
    WarehouseID INT,
    WarehouseName VARCHAR(100),
    Location VARCHAR(100),
    WarehouseType VARCHAR(50),
    ManagerKey INT,
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(WarehouseID)
DISTRIBUTED BY HASH(WarehouseID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_sales_territory_upsert (
    TerritoryID INT,
    TerritoryName VARCHAR(50),
    SalesRegion VARCHAR(50),
    Country VARCHAR(50),
    Manager VARCHAR(50),
    SalesTarget DECIMAL(18,2),
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(TerritoryID)
DISTRIBUTED BY HASH(TerritoryID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_customer_segment_upsert (
    SegmentID INT,
    SegmentName VARCHAR(50),
    SegmentDescription TEXT,
    DiscountTierStart DECIMAL(5,2),
    DiscountTierEnd DECIMAL(5,2),
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(SegmentID)
DISTRIBUTED BY HASH(SegmentID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_aging_tier_upsert (
    AgingTierID INT,
    AgingTierName VARCHAR(50),
    MinAgingDays INT,
    MaxAgingDays INT,
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(AgingTierID)
DISTRIBUTED BY HASH(AgingTierID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_finance_category_upsert (
    FinanceCategoryID INT,
    CategoryName VARCHAR(50),
    CategoryDescription TEXT,
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(FinanceCategoryID)
DISTRIBUTED BY HASH(FinanceCategoryID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_region_upsert (
    RegionID INT,
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    Continent VARCHAR(50),
    TimeZone VARCHAR(50),
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(RegionID)
DISTRIBUTED BY HASH(RegionID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_dim_product_category_upsert (
    ProductCategoryID INT,
    CategoryName VARCHAR(50),
    CategoryDescription TEXT,
    SourceUpdateDate DATE
) ENGINE=OLAP
DUPLICATE KEY(ProductCategoryID)
DISTRIBUTED BY HASH(ProductCategoryID) BUCKETS 5;



CREATE TABLE adventureworks_staging.stg_sales_returns_upsert (
    returnid BIGINT,
    returndate DATETIME,
    productid INT,
    customerid INT,
    reasonid INT,
    quantity INT,
    refund_amount DECIMAL(18,2),
    restocking_fee DECIMAL(10,2),
    upsert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
PRIMARY KEY (returnid)
DISTRIBUTED BY HASH(returnid) BUCKETS 5
PROPERTIES("replication_num" = "1");