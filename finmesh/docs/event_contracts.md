# FinMesh Event Contracts

FinMesh uses an event-driven architecture in which independent financial
networks communicate through Redpanda using the Kafka API.

This document describes the primary event topics used by the FinMesh
transaction lifecycle, their responsibilities, and the data exchanged between
services.

---

## Event Flow

The main transaction lifecycle is:

```text
raw.trade_orders
        ↓
approved.trade_orders
        ↓
exchange.trade_executions
        ↓
settlement.events
        ↓
custody.asset_movements
```

Rejected orders leave the normal lifecycle through:

```text
rejected.trade_orders
```

Lifecycle events are also consumed by Apache Flink, which derives the current
transaction state and publishes it to:

```text
reconciliation.results.v2
```

The persistence and intelligence layers consume these events independently.

---

# 1. raw.trade_orders

## Purpose

Represents a trade order submitted by the Bank Network before validation.

## Producer

```text
networks.bank_producer
```

## Primary Consumer

```text
core.validator_service
```

## Example Payload

```json
{
  "event_id": "EVT-...",
  "event_type": "TRADE_ORDER_CREATED",
  "trade_id": "TRD-...",
  "source_network": "BANK",
  "customer_id": "CUST-001",
  "asset": "MSFT",
  "side": "BUY",
  "quantity": 25,
  "price": 250.0,
  "currency": "USD",
  "event_timestamp": "2026-09-02T08:00:00+00:00"
}
```

## Notes

The Bank Network intentionally generates invalid data occasionally so that
FinMesh can exercise rejection paths.

Examples include:

```text
quantity <= 0
price <= 0
asset = UNKNOWN
```

---

# 2. approved.trade_orders

## Purpose

Represents an order that successfully passed FinMesh validation.

## Producer

```text
core.validator_service
```

## Consumers

Approved orders are used by downstream components including:

```text
Exchange Network
Flink reconciliation
ClickHouse Writer
Elasticsearch Writer
Ledger Writer
```

## Example Payload

```json
{
  "event_id": "EVT-...",
  "event_type": "TRADE_ORDER_APPROVED",
  "trade_id": "TRD-...",
  "source_network": "BANK",
  "customer_id": "CUST-001",
  "asset": "MSFT",
  "side": "BUY",
  "quantity": 25,
  "price": 250.0,
  "currency": "USD",
  "event_timestamp": "2026-09-02T08:00:01+00:00"
}
```

## Meaning

An approved event indicates that the trade passed the validation layer and may
continue through the simulated financial network.

---

# 3. rejected.trade_orders

## Purpose

Represents an order rejected by the Validator Service.

## Producer

```text
core.validator_service
```

## Example

A trade may be rejected because of conditions such as:

```text
invalid quantity
invalid price
unsupported asset
```

## Lifecycle Behavior

Rejected trades do not proceed to exchange execution, settlement, or custody.

This topic therefore represents an early termination path in the transaction
lifecycle.

---

# 4. exchange.trade_executions

## Purpose

Represents execution of an approved trade by the Exchange Network.

## Producer

```text
networks.exchange_network
```

## Downstream Consumers

```text
Settlement Network
Flink reconciliation
Elasticsearch Writer
Ledger Writer
```

## Example Payload

```json
{
  "event_id": "EVT-...",
  "event_type": "TRADE_EXECUTED",
  "execution_id": "EXE-78217",
  "trade_id": "TRD-DEMO-6C3E90",
  "source_network": "EXCHANGE",
  "asset": "MSFT",
  "quantity": 25,
  "execution_price": 248.99,
  "execution_timestamp": "2026-09-02T08:00:02+00:00"
}
```

## Meaning

The execution event establishes that an approved order reached the simulated
exchange and was executed.

The execution price may differ from the original order price.

---

# 5. settlement.events

## Purpose

Represents the settlement outcome for an executed transaction.

## Producer

```text
networks.settlement_network
```

## Downstream Consumers

```text
Custody Network
Flink reconciliation
Elasticsearch Writer
Ledger Writer
```

## Successful Example

```json
{
  "event_id": "EVT-...",
  "event_type": "SETTLEMENT_COMPLETED",
  "settlement_id": "SET-12345",
  "execution_id": "EXE-78217",
  "trade_id": "TRD-DEMO-6C3E90",
  "source_network": "SETTLEMENT",
  "status": "SETTLED",
  "reason": null,
  "settlement_timestamp": "2026-09-02T08:00:03+00:00"
}
```

## Failed Example

```json
{
  "event_id": "EVT-...",
  "event_type": "SETTLEMENT_FAILED",
  "settlement_id": "SET-25015",
  "execution_id": "EXE-78217",
  "trade_id": "TRD-DEMO-6C3E90",
  "source_network": "SETTLEMENT",
  "status": "FAILED",
  "reason": "counterparty_timeout",
  "settlement_timestamp": "2026-09-02T08:00:03+00:00"
}
```

## Settlement Statuses

The reconciliation layer currently distinguishes:

```text
SETTLED
FAILED
```

A failed settlement prevents the transaction from reaching a fully consistent
final state.

---

# 6. custody.asset_movements

## Purpose

Represents the custody-side asset movement associated with a settled
transaction.

## Producer

```text
networks.custody_network
```

## Downstream Consumers

```text
Flink reconciliation
Elasticsearch Writer
Ledger Writer
```

## Delivered Example

```json
{
  "event_id": "EVT-...",
  "event_type": "CUSTODY_MOVEMENT",
  "custody_event_id": "CST-...",
  "settlement_id": "SET-...",
  "trade_id": "TRD-...",
  "source_network": "CUSTODY",
  "status": "DELIVERED",
  "reason": null,
  "custody_timestamp": "2026-09-02T08:00:04+00:00"
}
```

## Blocked Example

```json
{
  "event_id": "EVT-...",
  "event_type": "CUSTODY_MOVEMENT",
  "custody_event_id": "CST-...",
  "settlement_id": "SET-...",
  "trade_id": "TRD-...",
  "source_network": "CUSTODY",
  "status": "BLOCKED",
  "reason": "asset_freeze",
  "custody_timestamp": "2026-09-02T08:00:04+00:00"
}
```

## Custody Statuses

The reconciliation layer currently distinguishes:

```text
DELIVERED
BLOCKED
```

---

# 7. reconciliation.results.v2

## Purpose

Represents the latest derived lifecycle state for a trade.

Unlike the individual network events, this event is not a new financial action.
It is a stateful interpretation of the events observed across the independent
FinMesh networks.

## Producer

```text
Apache Flink
```

## Source Streams

Flink derives reconciliation state from:

```text
approved.trade_orders
exchange.trade_executions
settlement.events
custody.asset_movements
```

## Sink Type

The topic uses the Flink:

```text
upsert-kafka
```

connector.

The logical primary key is:

```text
trade_id
```

This allows the reconciliation state for a transaction to evolve as new
lifecycle events arrive.

## Fields

```text
trade_id
has_approved_order
has_execution
settlement_status
custody_status
reconciliation_status
```

## Example

```json
{
  "trade_id": "TRD-DEMO-6C3E90",
  "has_approved_order": 1,
  "has_execution": 1,
  "settlement_status": "FAILED",
  "custody_status": null,
  "reconciliation_status": "SETTLEMENT_FAILED"
}
```

---

# Reconciliation State Model

FinMesh currently derives four primary reconciliation states.

## CONSISTENT

A transaction is consistent when all required lifecycle stages completed
successfully:

```text
approved order = true
execution = true
settlement = SETTLED
custody = DELIVERED
```

Result:

```text
CONSISTENT
```

---

## SETTLEMENT_FAILED

If settlement reports:

```text
FAILED
```

the reconciliation state becomes:

```text
SETTLEMENT_FAILED
```

This condition takes precedence over successful events from earlier lifecycle
stages.

---

## CUSTODY_BLOCKED

If settlement succeeds but custody reports:

```text
BLOCKED
```

the reconciliation state becomes:

```text
CUSTODY_BLOCKED
```

---

## PENDING

If the lifecycle has not yet produced enough events to determine a final
successful or failed state, reconciliation remains:

```text
PENDING
```

Because FinMesh is asynchronous, intermediate `PENDING` states are expected
while events are still moving through the network.

---

# Event Correlation

The primary identifier used to reconstruct a complete FinMesh transaction is:

```text
trade_id
```

The same trade ID is propagated across the financial network.

Additional identifiers represent domain-specific operations:

```text
trade_id
    ↓
execution_id
    ↓
settlement_id
    ↓
custody_event_id
```

This allows both end-to-end transaction analysis and domain-level traceability.

---

# Event Ordering

FinMesh is an asynchronous event-driven system.

Consumers should not assume that every lifecycle stage is immediately available
when an earlier event arrives.

The reconciliation layer therefore derives state incrementally as events become
available.

For transaction investigation, Elasticsearch lifecycle results are ordered
chronologically before being supplied to the FinMesh Intelligence layer.

---

# Persistence Consumers

FinMesh events are consumed by multiple independent persistence paths.

## PostgreSQL

The Ledger Writer stores events in a hash-chained audit ledger.

Its purpose is integrity and traceability rather than analytical querying.

## ClickHouse

The ClickHouse Writer stores analytical representations of approved trades and
reconciliation results.

Its purpose is high-speed aggregate analysis.

## Elasticsearch

The Elasticsearch Writer indexes lifecycle events by trade ID.

Its purpose is transaction-specific search and lifecycle reconstruction.

These systems intentionally serve different workloads.

---

# Intelligence Consumption

The FinMesh Intelligence layer does not treat the LLM as the source of
transaction truth.

For a transaction-specific question such as:

```text
Why did TRD-DEMO-6C3E90 fail?
```

FinMesh retrieves the underlying lifecycle events from Elasticsearch.

For an aggregate question such as:

```text
How many trades failed settlement?
```

FinMesh queries ClickHouse.

The resulting evidence is then supplied to the LLM for natural-language
interpretation.

This keeps factual transaction state grounded in FinMesh data rather than model
generation.

---

# Topic Summary

| Topic | Producer | Primary Purpose |
|---|---|---|
| `raw.trade_orders` | Bank Network | New trade submission |
| `approved.trade_orders` | Validator Service | Validated trade |
| `rejected.trade_orders` | Validator Service | Rejected trade |
| `exchange.trade_executions` | Exchange Network | Trade execution |
| `settlement.events` | Settlement Network | Settlement outcome |
| `custody.asset_movements` | Custody Network | Asset delivery/custody outcome |
| `reconciliation.results.v2` | Apache Flink | Derived lifecycle state |

---

# Design Principle

FinMesh event contracts separate the financial domains from one another.

The Bank Network does not need direct knowledge of settlement implementation.
The Exchange Network does not need direct access to custody storage.
Analytics systems do not need to participate in transaction processing.

Each component communicates through explicit event contracts.

This loose coupling allows the simulation to demonstrate how independently
operated financial networks can participate in a shared real-time transaction
lifecycle while remaining technically separated.