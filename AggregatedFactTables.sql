- **agg_daily_sales**
  - Aggregation: Daily sums of sales revenue, quantity, discount amount, transaction count
  - Dimensions: DimDate (SalesDateKey), DimStore, DimProductCategory
  - Grain: One row per store per product category per day
  - Update Frequency: Daily (post-midnight)

CREATE TABLE agg_daily_sales (
    SalesDateKey DATE,
    StoreKey BIGINT,
    ProductCategoryKey BIGINT,
    TotalRevenue DECIMAL(18,2),
    TotalQuantity INT,
    TotalDiscount DECIMAL(18,2),
    TransactionCount INT
)
DUPLICATE KEY (
    SalesDateKey, 
    StoreKey, 
    ProductCategoryKey
)    
PARTITION BY RANGE(SalesDateKey) (
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

  

- **agg_weekly_sales**
  - Aggregation: Weekly sales summary by product category and region (SUM, AVG, MIN, MAX)
  - Dimensions: DimDate (WeekStartDateKey), DimRegion, DimProductCategory
  - Grain: One row per region per product category per week
  - Update Frequency: Weekly (Sundays)

CREATE TABLE agg_weekly_sales (
    WeekStartDateKey DATE,
    RegionKey BIGINT,
    ProductCategoryKey BIGINT,
    SumRevenue DECIMAL(18,2),
    AvgRevenue DECIMAL(18,2),
    MinRevenue DECIMAL(18,2),
    MaxRevenue DECIMAL(18,2)
)
DUPLICATE KEY (
    WeekStartDateKey, 
    RegionKey, 
    ProductCategoryKey
)
PARTITION BY RANGE(WeekStartDateKey) (
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
DISTRIBUTED BY HASH(RegionKey) BUCKETS 10
PROPERTIES("replication_num" = "1");




- **agg_monthly_sales**
  - Aggregation: Monthly sales totals by customer segments (SUM revenue, AVG order value, distinct customer count)
  - Dimensions: DimDate (MonthStartDateKey), DimCustomerSegment, DimRegion
  - Grain: One row per customer segment per region per month
  - Update Frequency: Monthly (1st of next month)

CREATE TABLE agg_monthly_sales (
    MonthStartDateKey DATE,
    CustomerSegmentKey BIGINT,
    RegionKey BIGINT,
    TotalRevenue DECIMAL(18,2),
    AvgOrderValue DECIMAL(18,2),
    DistinctCustomerCount INT
)
DUPLICATE KEY (
    MonthStartDateKey, 
    CustomerSegmentKey, 
    RegionKey
)
PARTITION BY RANGE(MonthStartDateKey) (
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
DISTRIBUTED BY HASH(CustomerSegmentKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



- **agg_daily_inventory** (NEW)
  - Aggregation: Average inventory value by warehouse, product category, and aging tier
  - Dimensions: DimDate (InventoryDateKey), DimWarehouse, DimProductCategory, DimAgingTier
  - Grain: One row per warehouse per product category per aging tier per day
  - Update Frequency: Daily

CREATE TABLE agg_daily_inventory (
    InventoryDateKey DATE,
    WarehouseKey BIGINT,
    ProductCategoryKey BIGINT,
    AgingTierKey BIGINT,
    AvgInventoryValue DECIMAL(18,2)
)
DUPLICATE KEY (
    InventoryDateKey, 
    WarehouseKey, 
    ProductCategoryKey, 
    AgingTierKey
)
PARTITION BY RANGE(InventoryDateKey) (
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
DISTRIBUTED BY HASH(WarehouseKey) BUCKETS 10
PROPERTIES("replication_num" = "1");


- **agg_monthly_product_performance** (NEW)
  - Aggregation: Product performance metrics (revenue, units sold, returns rate, avg rating)
  - Dimensions: DimDate (MonthStartDateKey), DimProduct, DimStore
  - Grain: One row per product per store per month
  - Update Frequency: Monthly

CREATE TABLE agg_monthly_product_performance (
    MonthStartDateKey DATE,
    ProductKey BIGINT,
    StoreKey BIGINT,
    TotalRevenue DECIMAL(18,2),
    UnitsSold INT,
    ReturnRate DECIMAL(5,2),
    AvgRating DECIMAL(5,2)
)
DUPLICATE KEY (
    MonthStartDateKey, 
    ProductKey, 
    StoreKey
)
PARTITION BY RANGE(MonthStartDateKey) (
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


- **agg_regional_sales** (NEW)
  - Aggregation: Regional sales summary with growth rate calculations
  - Dimensions: DimDate (MonthStartDateKey), DimRegion, DimSalesTerritory
  - Grain: One row per region per territory per month
  - Update Frequency: Monthly

CREATE TABLE agg_regional_sales (
    MonthStartDateKey DATE,
    RegionKey BIGINT,
    SalesTerritoryKey BIGINT,
    TotalRevenue DECIMAL(18,2),
    GrowthRate DECIMAL(5,2)
)
DUPLICATE KEY (
    MonthStartDateKey, 
    RegionKey, 
    SalesTerritoryKey
)
PARTITION BY RANGE(MonthStartDateKey) (
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
DISTRIBUTED BY HASH(RegionKey) BUCKETS 10
PROPERTIES("replication_num" = "1");
