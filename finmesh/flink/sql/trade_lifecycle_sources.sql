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
    'properties.group.id' = 'finmesh-flink-exchange',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
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
    'properties.group.id' = 'finmesh-flink-settlement',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
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
    'properties.group.id' = 'finmesh-flink-custody',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);