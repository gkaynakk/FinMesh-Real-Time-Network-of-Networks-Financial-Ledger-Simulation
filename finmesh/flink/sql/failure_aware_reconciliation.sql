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
    'properties.group.id' = 'finmesh-flink-recon-orders',
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
    'properties.group.id' = 'finmesh-flink-recon-executions',
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
    'properties.group.id' = 'finmesh-flink-recon-settlement',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE custody_asset_movements (
    event_id STRING,
    event_type STRING,
    custody_event_id STRING,
    settlement_id STRING,
    trade_id STRING,
    source_network STRING,
    status STRING,
    reason STRING,
    custody_timestamp STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'custody.asset_movements',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'finmesh-flink-recon-custody',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

SELECT
    trade_id,

    MAX(approved_flag) AS has_approved_order,
    MAX(execution_flag) AS has_execution,

    CASE
        WHEN MAX(settlement_failed) = 1 THEN 'FAILED'
        WHEN MAX(settlement_settled) = 1 THEN 'SETTLED'
        ELSE NULL
    END AS settlement_status,

    CASE
        WHEN MAX(custody_blocked) = 1 THEN 'BLOCKED'
        WHEN MAX(custody_delivered) = 1 THEN 'DELIVERED'
        ELSE NULL
    END AS custody_status,

    CASE
        WHEN MAX(settlement_failed) = 1
            THEN 'SETTLEMENT_FAILED'

        WHEN MAX(custody_blocked) = 1
            THEN 'CUSTODY_BLOCKED'

        WHEN MAX(approved_flag) = 1
             AND MAX(execution_flag) = 1
             AND MAX(settlement_settled) = 1
             AND MAX(custody_delivered) = 1
            THEN 'CONSISTENT'

        ELSE 'PENDING'
    END AS reconciliation_status

FROM (

    SELECT
        trade_id,
        1 AS approved_flag,
        0 AS execution_flag,
        0 AS settlement_settled,
        0 AS settlement_failed,
        0 AS custody_delivered,
        0 AS custody_blocked
    FROM approved_trade_orders

    UNION ALL

    SELECT
        trade_id,
        0,
        1,
        0,
        0,
        0,
        0
    FROM exchange_trade_executions

    UNION ALL

    SELECT
        trade_id,
        0,
        0,
        CASE WHEN status = 'SETTLED' THEN 1 ELSE 0 END,
        CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END,
        0,
        0
    FROM settlement_events

    UNION ALL

    SELECT
        trade_id,
        0,
        0,
        0,
        0,
        CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END,
        CASE WHEN status = 'BLOCKED' THEN 1 ELSE 0 END
    FROM custody_asset_movements

) lifecycle_events

GROUP BY trade_id;