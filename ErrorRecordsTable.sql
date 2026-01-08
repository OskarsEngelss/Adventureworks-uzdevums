CREATE TABLE error_records (
    ErrorID BIGINT,
    ErrorDate DATETIME,
    SourceTable VARCHAR(100),
    RecordNaturalKey VARCHAR(100),
    ErrorType VARCHAR(50),
    ErrorSeverity VARCHAR(50),
    ErrorMessage VARCHAR(255),
    ErrorDetails TEXT,
    FailedData TEXT,
    ProcessingBatchID VARCHAR(50),
    TaskName VARCHAR(100),
    IsRecoverable BOOLEAN,
    RetryCount INT,
    LastAttemptDate DATETIME,
    IsResolved BOOLEAN,
    ResolutionComment VARCHAR(255) NULL
)
DUPLICATE KEY(ErrorID)
PARTITION BY RANGE(ErrorDate) (
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
DISTRIBUTED BY HASH(ErrorID) BUCKETS 10
PROPERTIES("replication_num" = "1");





CREATE TABLE adventureworks.StagingCustomer (
    customerid BIGINT,
    customername VARCHAR(200),
    emailaddress VARCHAR(200),
    phonenumber VARCHAR(50),
    city VARCHAR(100),
    stateprovinceid BIGINT,
    postalcode VARCHAR(20),
    customersegment VARCHAR(50),
    customertype VARCHAR(50),
    accountstatus VARCHAR(50),
    creditlimit BIGINT,
    annualincome BIGINT,
    yearssincefirstpurchase BIGINT
)
DUPLICATE KEY(customerid)
DISTRIBUTED BY HASH(customerid)
PROPERTIES ("replication_num" = "1");