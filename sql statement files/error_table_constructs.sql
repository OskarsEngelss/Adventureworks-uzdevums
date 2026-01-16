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