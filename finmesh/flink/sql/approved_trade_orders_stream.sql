SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE approved_trade_orders (
    event_id STRING,
    event_type STRING,
    trade_id STRING,
    source_network STRING,
    customer_id STRING,
    asset STRING,
    side STRING,
    quantity INT,
    price DOUBLE,
    currency STRING,
    event_timestamp STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'approved.trade_orders',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'finmesh-flink-approved-orders',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

SELECT
    trade_id,
    asset,
    side,
    quantity,
    price
FROM approved_trade_orders;