CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
    transaction_id STRING,
    account_id STRING,
    amount DOUBLE,
    transaction_type STRING,
    location STRING,
    `timestamp` STRING,
    is_fraud BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/transactions/';
