CREATE DATABASE IF NOT EXISTS finmesh;

CREATE TABLE IF NOT EXISTS finmesh.approved_trade_events (
    event_id String,
    trade_id String,
    customer_id String,
    asset String,
    side String,
    quantity Int32,
    price Float64,
    source_network String,
    event_timestamp DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (event_timestamp, trade_id);

CREATE TABLE IF NOT EXISTS finmesh.reconciliation_results (
    trade_id String,
    has_approved_order Bool,
    has_execution Bool,
    settlement_status Nullable(String),
    custody_status Nullable(String),
    is_complete Bool,
    reconciliation_status String,
    inserted_at DateTime64(3) DEFAULT now64(3)¬
)
ENGINE = MergeTree
ORDER BY (inserted_at, trade_id);