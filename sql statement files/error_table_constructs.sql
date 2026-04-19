CREATE TABLE adventureworks_errors.fact_sales_errors (
    ErrorTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    SalesOrderID INT,
    SalesOrderDetailID INT,
    FailureReason VARCHAR(100),
    IsRecoverable TINYINT,
    FailedData JSON
) ENGINE=OLAP
DUPLICATE KEY(ErrorTimestamp, SalesOrderID)
DISTRIBUTED BY HASH(SalesOrderID) BUCKETS 5
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.fact_purchases_errors (
    PurchaseOrderID INT,
    PurchaseOrderDetailID INT,
    FailureReason VARCHAR(255),
    IsRecoverable TINYINT,
    FailedData JSON,
    ErrorTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP
) 
DUPLICATE KEY(PurchaseOrderID)
DISTRIBUTED BY HASH(PurchaseOrderID) BUCKETS 3
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.fact_inventory_errors (
    ProductID INT,
    LocationID INT,
    FailureReason VARCHAR(255),
    IsRecoverable TINYINT,
    FailedData JSON,
    ErrorTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP
) 
DUPLICATE KEY(ProductID)
DISTRIBUTED BY HASH(ProductID) BUCKETS 3
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.fact_production_errors (
    WorkOrderID INT,
    FailureReason VARCHAR(255),
    IsRecoverable TINYINT,
    FailedData JSON,
    ErrorTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP
) 
DUPLICATE KEY(WorkOrderID)
DISTRIBUTED BY HASH(WorkOrderID) BUCKETS 5
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.fact_employeesales_errors (
    ErrorID BIGINT AUTO_INCREMENT,
    SalesOrderID INT,
    FailureReason VARCHAR(255),
    IsRecoverable TINYINT,
    FailedData JSON,
    ErrorTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(ErrorID)
DISTRIBUTED BY HASH(ErrorID) BUCKETS 5
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.fact_customerfeedback_errors (
    ErrorID BIGINT AUTO_INCREMENT,
    SourceFeedbackID BIGINT,
    FailureReason VARCHAR(255),
    IsRecoverable TINYINT,
    FailedData JSON,
    ErrorTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(ErrorID)
DISTRIBUTED BY HASH(ErrorID) BUCKETS 5
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.fact_promotionresponse_errors (
    ErrorID BIGINT AUTO_INCREMENT,
    SourceSalesOrderID INT,
    SourceSalesOrderDetailID INT,
    FailureReason VARCHAR(255),
    IsRecoverable TINYINT,
    FailedData JSON,
    ErrorTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(ErrorID)
DISTRIBUTED BY HASH(ErrorID) BUCKETS 5
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.fact_finance_errors (
    InvoiceID BIGINT,
    FailureReason VARCHAR(100),
    IsRecoverable TINYINT,
    FailedData JSON
)
DUPLICATE KEY(InvoiceID)
DISTRIBUTED BY HASH(InvoiceID) BUCKETS 5
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.fact_returns_errors (
    ReturnID BIGINT,
    FailureReason VARCHAR(255),
    IsRecoverable BOOLEAN,
    FailedData JSON,
    ErrorTimestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY (ReturnID)
DISTRIBUTED BY HASH(ReturnID) BUCKETS 5
PROPERTIES("replication_num" = "1");


-- Aggregated Fact Table Error Logs
-- Aggregated Fact Table Error Logs
-- Aggregated Fact Table Error Logs


CREATE TABLE adventureworks_errors.agg_daily_sales_errors (
    ErrorID BIGINT AUTO_INCREMENT,
    SalesDateKey BIGINT,
    FailureReason VARCHAR(255),
    FailedAt DATETIME,
    SQLState VARCHAR(20)
) 
ENGINE=OLAP
DUPLICATE KEY(ErrorID, SalesDateKey)
DISTRIBUTED BY HASH(SalesDateKey) BUCKETS 1
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.agg_weekly_sales_errors (
    ErrorID BIGINT AUTO_INCREMENT,
    WeekStartDateKey BIGINT,
    FailureReason VARCHAR(255),
    FailedAt DATETIME,
    SQLState VARCHAR(20)
) 
ENGINE=OLAP
DUPLICATE KEY(ErrorID, WeekStartDateKey)
DISTRIBUTED BY HASH(WeekStartDateKey) BUCKETS 1
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.agg_monthly_sales_errors (
    ErrorID BIGINT AUTO_INCREMENT,
    MonthStartDateKey BIGINT,
    FailureReason VARCHAR(255),
    FailedAt DATETIME,
    SQLState VARCHAR(20)
) 
ENGINE=OLAP
DUPLICATE KEY(ErrorID)
DISTRIBUTED BY HASH(ErrorID) BUCKETS 1
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.agg_daily_inventory_errors (
    ErrorID BIGINT AUTO_INCREMENT,
    InventoryDateKey BIGINT,
    FailureReason VARCHAR(255),
    FailedAt DATETIME,
    SQLState VARCHAR(20)
) 
ENGINE=OLAP
DUPLICATE KEY(ErrorID)
DISTRIBUTED BY HASH(ErrorID) BUCKETS 1
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.agg_monthly_product_performance_errors (
    ErrorID BIGINT AUTO_INCREMENT,
    MonthStartDateKey BIGINT,
    FailureReason VARCHAR(255),
    FailedAt DATETIME,
    SQLState VARCHAR(20)
) 
ENGINE=OLAP
DUPLICATE KEY(ErrorID)
DISTRIBUTED BY HASH(ErrorID) BUCKETS 1
PROPERTIES("replication_num" = "1");



CREATE TABLE adventureworks_errors.agg_regional_sales_errors (
    ErrorID BIGINT AUTO_INCREMENT,
    MonthStartDateKey BIGINT,
    FailureReason VARCHAR(255),
    FailedAt DATETIME,
    SQLState VARCHAR(20)
) 
ENGINE=OLAP
DUPLICATE KEY(ErrorID)
DISTRIBUTED BY HASH(ErrorID) BUCKETS 1
PROPERTIES("replication_num" = "1");











CREATE TABLE adventureworks_errors.error_records (
    ErrorID BIGINT AUTO_INCREMENT,
    ErrorDate DATETIME,
    SourceTable VARCHAR(100),
    RecordNaturalKey VARCHAR(255),
    FailureReason VARCHAR(255),
    FailedData JSON,
    IsRecoverable TINYINT,
    RetryCount INT,
    IsResolved TINYINT,
    LastAttemptDate DATETIME,
    ResolutionComment VARCHAR(500)
) ENGINE=OLAP
PRIMARY KEY(ErrorID)
DISTRIBUTED BY HASH(ErrorID) BUCKETS 3;