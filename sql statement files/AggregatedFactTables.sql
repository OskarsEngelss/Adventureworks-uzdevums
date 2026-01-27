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
    PARTITION p2025 VALUES LESS THAN ('2026-01-01'),
    PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
    PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
    PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
    PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
    PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
    PARTITION p202606 VALUES LESS THAN ('2026-07-01'),
    PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
    PARTITION p202608 VALUES LESS THAN ('2026-09-01'),
    PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
    PARTITION p202610 VALUES LESS THAN ('2026-11-01'),
    PARTITION p202611 VALUES LESS THAN ('2026-12-01'),
    PARTITION p202612 VALUES LESS THAN ('2027-01-01')
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
    PARTITION p2025 VALUES LESS THAN ('2026-01-01'),
    PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
    PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
    PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
    PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
    PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
    PARTITION p202606 VALUES LESS THAN ('2026-07-01'),
    PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
    PARTITION p202608 VALUES LESS THAN ('2026-09-01'),
    PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
    PARTITION p202610 VALUES LESS THAN ('2026-11-01'),
    PARTITION p202611 VALUES LESS THAN ('2026-12-01'),
    PARTITION p202612 VALUES LESS THAN ('2027-01-01')
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
    PARTITION p2025 VALUES LESS THAN ('2026-01-01'),
    PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
    PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
    PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
    PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
    PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
    PARTITION p202606 VALUES LESS THAN ('2026-07-01'),
    PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
    PARTITION p202608 VALUES LESS THAN ('2026-09-01'),
    PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
    PARTITION p202610 VALUES LESS THAN ('2026-11-01'),
    PARTITION p202611 VALUES LESS THAN ('2026-12-01'),
    PARTITION p202612 VALUES LESS THAN ('2027-01-01')
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
    PARTITION p2025 VALUES LESS THAN ('2026-01-01'),
    PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
    PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
    PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
    PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
    PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
    PARTITION p202606 VALUES LESS THAN ('2026-07-01'),
    PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
    PARTITION p202608 VALUES LESS THAN ('2026-09-01'),
    PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
    PARTITION p202610 VALUES LESS THAN ('2026-11-01'),
    PARTITION p202611 VALUES LESS THAN ('2026-12-01'),
    PARTITION p202612 VALUES LESS THAN ('2027-01-01')
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
    PARTITION p2025 VALUES LESS THAN ('2026-01-01'),
    PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
    PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
    PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
    PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
    PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
    PARTITION p202606 VALUES LESS THAN ('2026-07-01'),
    PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
    PARTITION p202608 VALUES LESS THAN ('2026-09-01'),
    PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
    PARTITION p202610 VALUES LESS THAN ('2026-11-01'),
    PARTITION p202611 VALUES LESS THAN ('2026-12-01'),
    PARTITION p202612 VALUES LESS THAN ('2027-01-01')
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
    GrowthRate DECIMAL(12,2)
)
DUPLICATE KEY (
    MonthStartDateKey, 
    RegionKey, 
    SalesTerritoryKey
)
PARTITION BY RANGE(MonthStartDateKey) (
    PARTITION p2025 VALUES LESS THAN ('2026-01-01'),
    PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
    PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
    PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
    PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
    PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
    PARTITION p202606 VALUES LESS THAN ('2026-07-01'),
    PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
    PARTITION p202608 VALUES LESS THAN ('2026-09-01'),
    PARTITION p202609 VALUES LESS THAN ('2026-10-01'),
    PARTITION p202610 VALUES LESS THAN ('2026-11-01'),
    PARTITION p202611 VALUES LESS THAN ('2026-12-01'),
    PARTITION p202612 VALUES LESS THAN ('2027-01-01')
)
DISTRIBUTED BY HASH(RegionKey) BUCKETS 10
PROPERTIES("replication_num" = "1");
