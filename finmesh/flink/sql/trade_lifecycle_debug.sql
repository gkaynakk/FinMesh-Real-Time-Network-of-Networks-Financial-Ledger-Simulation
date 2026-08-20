SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE exchange_trade_executions (
    event_id STRING,
    event_type STRING,
    execution_id STRING,
    trade_id STRING,
    source_network STRING,
    asset STRING,
    quantity INT,
    execution_price DOUBLE,
    execution_timestamp STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'exchange.trade_executions',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'finmesh-flink-exchange-debug',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

SELECT
    trade_id,
    execution_id,
    asset,
    quantity,
    execution_price
FROM exchange_trade_executions;