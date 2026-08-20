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
    'properties.group.id' = 'finmesh-flink-approved-settlement',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

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
    'properties.group.id' = 'finmesh-flink-exchange-settlement',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE settlement_events (
    event_id STRING,
    event_type STRING,
    settlement_id STRING,
    execution_id STRING,
    trade_id STRING,
    source_network STRING,
    status STRING,
    reason STRING,
    settlement_timestamp STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'settlement.events',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'finmesh-flink-settlement-join',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

SELECT
    o.trade_id,
    o.customer_id,
    o.asset,
    o.side,
    o.quantity AS ordered_quantity,
    o.price AS order_price,
    e.execution_id,
    e.execution_price,
    s.settlement_id,
    s.status AS settlement_status,
    s.reason AS settlement_reason,
    CASE
        WHEN s.status = 'SETTLED' THEN 'SETTLED'
        WHEN s.status = 'FAILED' THEN 'SETTLEMENT_FAILED'
        ELSE 'PENDING'
    END AS lifecycle_status
FROM approved_trade_orders o
JOIN exchange_trade_executions e
    ON o.trade_id = e.trade_id
JOIN settlement_events s
    ON e.execution_id = s.execution_id
    AND e.trade_id = s.trade_id;