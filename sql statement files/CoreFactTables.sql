**FactSales**
  - Metrics: Sales Revenue, Quantity Sold, Discount Amount, Number of Transactions
  - Dimensions: DimDate (SalesDateKey), DimCustomer, DimProduct, DimStore, DimEmployee
  - Grain: One row per order detail line item
  - Partitioning: By SalesDateKey

CREATE TABLE adventureworks.FactSales (
    SalesDateKey BIGINT,
    CustomerKey BIGINT,
    ProductKey BIGINT,
    StoreKey BIGINT,
    EmployeeKey BIGINT,
    SalesRevenue DECIMAL(18,2),
    QuantitySold INT,
    DiscountAmount DECIMAL(18,2)
)
DUPLICATE KEY ( 
    SalesDateKey,
    CustomerKey,
    ProductKey,
    StoreKey,
    EmployeeKey
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
DISTRIBUTED BY HASH(CustomerKey) BUCKETS 10 
PROPERTIES("replication_num" = "1");



**FactPurchases**
  - Metrics: Purchase Amount, Purchase Quantity, Discounts, Unit Cost
  - Dimensions: DimDate (PurchaseDateKey), DimProduct, DimVendor
  - Grain: One row per purchase order line item
  - Partitioning: By PurchaseDateKey

CREATE TABLE FactPurchases (
    PurchaseDateKey BIGINT,
    ProductKey BIGINT,
    VendorKey BIGINT,
    PurchaseAmount DECIMAL(18,2),
    PurchaseQuantity INT,
    DiscountAmount DECIMAL(18,2),
    UnitCost DECIMAL(18,2)
)
DUPLICATE KEY (
    PurchaseDateKey,
    ProductKey,
    VendorKey
)
PARTITION BY RANGE(PurchaseDateKey) (
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
DISTRIBUTED BY HASH(PurchaseDateKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



- **FactInventory**
  - Metrics: Quantity on Hand, Stock Aging (days), Reorder Levels, Safety Stock
  - Dimensions: DimDate (InventoryDateKey), DimProduct, DimStore, DimWarehouse
  - Grain: One row per product per warehouse per day
  - Partitioning: By InventoryDateKey
  - Note: This is a **factless fact table** with time-series snapshots

CREATE TABLE FactInventory (
    InventoryDateKey BIGINT,
    ProductKey BIGINT,
    StoreKey BIGINT,
    WarehouseKey BIGINT,
    QuantityOnHand INT,
    StockAgingDays INT,
    ReorderLevel INT,
    SafetyStock INT
)
DUPLICATE KEY (
    InventoryDateKey,
    ProductKey,
    StoreKey,
    WarehouseKey
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
DISTRIBUTED BY HASH(InventoryDateKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



- **FactProduction**
  - Metrics: Units Produced, Production Time (hours), Scrap Rate (%), Defect Count
  - Dimensions: DimDate (ProductionDateKey), DimProduct, DimEmployee (supervisor)
  - Grain: One row per production run/batch
  - Partitioning: By ProductionDateKey

CREATE TABLE FactProduction (
    ProductionDateKey BIGINT,
    ProductKey BIGINT,
    EmployeeKey BIGINT,
    ProductionTimeHours DECIMAL(10,2),
    UnitsProduced INT,
    ScrapRate DECIMAL(5,2),
    DefectCount INT
)
DUPLICATE KEY (
    ProductionDateKey,
    ProductKey,
    EmployeeKey,
    ProductionTimeHours
)
PARTITION BY RANGE(ProductionDateKey) (
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
DISTRIBUTED BY HASH(ProductionDateKey) BUCKETS 10
PROPERTIES("replication_num" = "1");




- **FactEmployeeSales**
  - Metrics: Sales Amount by Employee, Sales Target vs. Actual, Customer Contacts Count
  - Dimensions: DimDate (SalesDateKey), DimEmployee, DimStore, DimSalesTerritory
  - Grain: One row per employee per day
  - Partitioning: By SalesDateKey

CREATE TABLE FactEmployeeSales (
    SalesDateKey BIGINT,
    EmployeeKey BIGINT,
    SalesTerritoryKey BIGINT,
    StoreKey BIGINT,
    SalesAmount DECIMAL(18,2),
    SalesTarget DECIMAL(18,2),
    CustomerContactsCount INT
)
UNIQUE KEY (
    SalesDateKey,
    EmployeeKey,
    SalesTerritoryKey
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
DISTRIBUTED BY HASH(SalesDateKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



- **FactCustomerFeedback**
  - Metrics: Feedback Scores (1-5), Complaint Counts, Resolution Times (hours), CSAT Score
  - Dimensions: DimDate (FeedbackDateKey), DimCustomer, DimEmployee (handler), DimFeedbackCategory
  - Grain: One row per feedback submission
  - Partitioning: By FeedbackDateKey

CREATE TABLE FactCustomerFeedback (
    FeedbackID BIGINT,
    FeedbackDateKey BIGINT,
    CustomerKey BIGINT,
    EmployeeKey BIGINT,
    FeedbackCategoryKey BIGINT,
    FeedbackScore INT,
    ComplaintCount INT,
    ResolutionTimeHours DECIMAL(10,2),
    CSATScore DECIMAL(5,2)
)
DUPLICATE KEY (FeedbackID)
PARTITION BY RANGE(FeedbackDateKey) (
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
DISTRIBUTED BY HASH(CustomerKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



- **FactPromotionResponse**
  - Metrics: Sales During Campaign, Discount Usage Count, Customer Uptake Rate (%), Promotion ROI
  - Dimensions: DimDate (PromotionDateKey), DimProduct, DimStore, DimPromotion
  - Grain: One row per product per promotion per store per day
  - Partitioning: By PromotionDateKey

CREATE TABLE FactPromotionResponse (
    PromotionDateKey BIGINT,
    ProductKey BIGINT,
    StoreKey BIGINT,
    PromotionKey BIGINT,
    SalesDuringCampaign DECIMAL(18,2),
    DiscountUsageCount INT,
    CustomerUptakeRate DECIMAL(5,2),
    PromotionROI DECIMAL(10,2)
)
DUPLICATE KEY (
    PromotionDateKey,
    ProductKey,
    StoreKey,
    PromotionKey
)
PARTITION BY RANGE(PromotionDateKey) (
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
DISTRIBUTED BY HASH(PromotionDateKey) BUCKETS 10
PROPERTIES("replication_num" = "1");



- **FactFinance**
  - Metrics: Invoice Amounts, Payment Delays (days), Credit Usage (%), Interest Charges
  - Dimensions: DimDate (InvoiceDateKey), DimCustomer, DimStore, DimFinanceCategory
  - Grain: One row per invoice
  - Partitioning: By InvoiceDateKey

CREATE TABLE FactFinance (
    InvoiceID BIGINT,
    InvoiceDateKey BIGINT,
    CustomerKey BIGINT,
    StoreKey BIGINT,
    FinanceCategoryKey BIGINT,
    InvoiceAmount DECIMAL(18,2),
    PaymentDelayDays INT,
    CreditUsage DECIMAL(5,2),
    InterestCharges DECIMAL(10,2)
)
DUPLICATE KEY (InvoiceID)
PARTITION BY RANGE(InvoiceDateKey) (
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
DISTRIBUTED BY HASH(InvoiceDateKey) BUCKETS 10
PROPERTIES("replication_num" = "1");


- **FactReturns**
  - Metrics: Returned Quantity, Refund Amount, Return Reasons, Restocking Fee
  - Dimensions: DimDate (ReturnDateKey), DimProduct, DimCustomer, DimStore, DimReturnReason
  - Grain: One row per return line item
  - Partitioning: By ReturnDateKey

CREATE TABLE FactReturns (
    ReturnID BIGINT,
    ReturnDateKey BIGINT,
    ProductKey BIGINT,
    CustomerKey BIGINT,
    StoreKey BIGINT,
    ReturnReasonKey BIGINT,
    ReturnedQuantity INT,
    RefundAmount DECIMAL(18,2),
    RestockingFee DECIMAL(10,2)
)
DUPLICATE KEY (ReturnID)
PARTITION BY RANGE(ReturnDateKey) (
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
DISTRIBUTED BY HASH(ReturnDateKey) BUCKETS 10
PROPERTIES("replication_num" = "1");