- **agg_daily_sales**
  - Aggregation: Daily sums of sales revenue, quantity, discount amount, transaction count
  - Dimensions: DimDate (SalesDateKey), DimStore, DimProductCategory
  - Grain: One row per store per product category per day
  - Update Frequency: Daily (post-midnight)

CREATE TABLE agg_daily_sales (
    SalesDateKey BIGINT,
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
    PARTITION p_history VALUES LESS THAN ("20260101"),
    PARTITION p202601 VALUES LESS THAN ("20260201"),
    PARTITION p202602 VALUES LESS THAN ("20260301"),
    PARTITION p202603 VALUES LESS THAN ("20260401"),
    PARTITION p202604 VALUES LESS THAN ("20260501"),
    PARTITION p202605 VALUES LESS THAN ("20260601"),
    PARTITION p202606 VALUES LESS THAN ("20260701"),
    PARTITION p202607 VALUES LESS THAN ("20260801"),
    PARTITION p202608 VALUES LESS THAN ("20260901"),
    PARTITION p202609 VALUES LESS THAN ("20261001"),
    PARTITION p202610 VALUES LESS THAN ("20261101"),
    PARTITION p202611 VALUES LESS THAN ("20261201"),
    PARTITION p202612 VALUES LESS THAN ("20270101")
)
DISTRIBUTED BY HASH(StoreKey) BUCKETS 10
PROPERTIES("replication_num" = "1");

  

- **agg_weekly_sales**
  - Aggregation: Weekly sales summary by product category and region (SUM, AVG, MIN, MAX)
  - Dimensions: DimDate (WeekStartDateKey), DimRegion, DimProductCategory
  - Grain: One row per region per product category per week
  - Update Frequency: Weekly (Sundays)

CREATE TABLE agg_weekly_sales (
    WeekStartDateKey BIGINT,
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
    PARTITION p_history VALUES LESS THAN ("20260101"),
    PARTITION p202601 VALUES LESS THAN ("20260201"),
    PARTITION p202602 VALUES LESS THAN ("20260301"),
    PARTITION p202603 VALUES LESS THAN ("20260401"),
    PARTITION p202604 VALUES LESS THAN ("20260501"),
    PARTITION p202605 VALUES LESS THAN ("20260601"),
    PARTITION p202606 VALUES LESS THAN ("20260701"),
    PARTITION p202607 VALUES LESS THAN ("20260801"),
    PARTITION p202608 VALUES LESS THAN ("20260901"),
    PARTITION p202609 VALUES LESS THAN ("20261001"),
    PARTITION p202610 VALUES LESS THAN ("20261101"),
    PARTITION p202611 VALUES LESS THAN ("20261201"),
    PARTITION p202612 VALUES LESS THAN ("20270101")
)
DISTRIBUTED BY HASH(RegionKey) BUCKETS 10
PROPERTIES("replication_num" = "1");




- **agg_monthly_sales**
  - Aggregation: Monthly sales totals by customer segments (SUM revenue, AVG order value, distinct customer count)
  - Dimensions: DimDate (MonthStartDateKey), DimCustomerSegment, DimRegion
  - Grain: One row per customer segment per region per month
  - Update Frequency: Monthly (1st of next month)

CREATE TABLE agg_monthly_sales (
    MonthStartDateKey BIGINT,
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
    PARTITION p_history VALUES LESS THAN ("20260101"),
    PARTITION p202601 VALUES LESS THAN ("20260201"),
    PARTITION p202602 VALUES LESS THAN ("20260301"),
    PARTITION p202603 VALUES LESS THAN ("20260401"),
    PARTITION p202604 VALUES LESS THAN ("20260501"),
    PARTITION p202605 VALUES LESS THAN ("20260601"),
    PARTITION p202606 VALUES LESS THAN ("20260701"),
    PARTITION p202607 VALUES LESS THAN ("20260801"),
    PARTITION p202608 VALUES LESS THAN ("20260901"),
    PARTITION p202609 VALUES LESS THAN ("20261001"),
    PARTITION p202610 VALUES LESS THAN ("20261101"),
    PARTITION p202611 VALUES LESS THAN ("20261201"),
    PARTITION p202612 VALUES LESS THAN ("20270101")
)
DISTRIBUTED BY HASH(CustomerSegmentKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



- **agg_daily_inventory** (NEW)
  - Aggregation: Average inventory value by warehouse, product category, and aging tier
  - Dimensions: DimDate (InventoryDateKey), DimWarehouse, DimProductCategory, DimAgingTier
  - Grain: One row per warehouse per product category per aging tier per day
  - Update Frequency: Daily

CREATE TABLE agg_daily_inventory (
    InventoryDateKey BIGINT,
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
    PARTITION p_history VALUES LESS THAN ("20260101"),
    PARTITION p202601 VALUES LESS THAN ("20260201"),
    PARTITION p202602 VALUES LESS THAN ("20260301"),
    PARTITION p202603 VALUES LESS THAN ("20260401"),
    PARTITION p202604 VALUES LESS THAN ("20260501"),
    PARTITION p202605 VALUES LESS THAN ("20260601"),
    PARTITION p202606 VALUES LESS THAN ("20260701"),
    PARTITION p202607 VALUES LESS THAN ("20260801"),
    PARTITION p202608 VALUES LESS THAN ("20260901"),
    PARTITION p202609 VALUES LESS THAN ("20261001"),
    PARTITION p202610 VALUES LESS THAN ("20261101"),
    PARTITION p202611 VALUES LESS THAN ("20261201"),
    PARTITION p202612 VALUES LESS THAN ("20270101")
)
DISTRIBUTED BY HASH(WarehouseKey) BUCKETS 10
PROPERTIES("replication_num" = "1");


- **agg_monthly_product_performance** (NEW)
  - Aggregation: Product performance metrics (revenue, units sold, returns rate, avg rating)
  - Dimensions: DimDate (MonthStartDateKey), DimProduct, DimStore
  - Grain: One row per product per store per month
  - Update Frequency: Monthly

CREATE TABLE agg_monthly_product_performance (
    MonthStartDateKey BIGINT,
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
    PARTITION p_history VALUES LESS THAN ("20260101"),
    PARTITION p202601 VALUES LESS THAN ("20260201"),
    PARTITION p202602 VALUES LESS THAN ("20260301"),
    PARTITION p202603 VALUES LESS THAN ("20260401"),
    PARTITION p202604 VALUES LESS THAN ("20260501"),
    PARTITION p202605 VALUES LESS THAN ("20260601"),
    PARTITION p202606 VALUES LESS THAN ("20260701"),
    PARTITION p202607 VALUES LESS THAN ("20260801"),
    PARTITION p202608 VALUES LESS THAN ("20260901"),
    PARTITION p202609 VALUES LESS THAN ("20261001"),
    PARTITION p202610 VALUES LESS THAN ("20261101"),
    PARTITION p202611 VALUES LESS THAN ("20261201"),
    PARTITION p202612 VALUES LESS THAN ("20270101")
)
DISTRIBUTED BY HASH(ProductKey) BUCKETS 10
PROPERTIES("replication_num" = "1");


- **agg_regional_sales** (NEW)
  - Aggregation: Regional sales summary with growth rate calculations
  - Dimensions: DimDate (MonthStartDateKey), DimRegion, DimSalesTerritory
  - Grain: One row per region per territory per month
  - Update Frequency: Monthly

CREATE TABLE agg_regional_sales (
    MonthStartDateKey BIGINT,
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
    PARTITION p_history VALUES LESS THAN ("20260101"),
    PARTITION p202601 VALUES LESS THAN ("20260201"),
    PARTITION p202602 VALUES LESS THAN ("20260301"),
    PARTITION p202603 VALUES LESS THAN ("20260401"),
    PARTITION p202604 VALUES LESS THAN ("20260501"),
    PARTITION p202605 VALUES LESS THAN ("20260601"),
    PARTITION p202606 VALUES LESS THAN ("20260701"),
    PARTITION p202607 VALUES LESS THAN ("20260801"),
    PARTITION p202608 VALUES LESS THAN ("20260901"),
    PARTITION p202609 VALUES LESS THAN ("20261001"),
    PARTITION p202610 VALUES LESS THAN ("20261101"),
    PARTITION p202611 VALUES LESS THAN ("20261201"),
    PARTITION p202612 VALUES LESS THAN ("20270101")
)
DISTRIBUTED BY HASH(RegionKey) BUCKETS 10
PROPERTIES("replication_num" = "1");
