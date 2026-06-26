import json
from datetime import datetime
from typing import Any

import clickhouse_connect

from shared.kafka import create_consumer

consumer = create_consumer("clickhouse-writer")


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="finmesh",
        password="finmesh",
        database="finmesh",
    )


def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def insert_approved_trade(client, event: dict[str, Any]) -> None:
    client.insert(
        "approved_trade_events",
        [
            [
                event["event_id"],
                event["trade_id"],
                event["customer_id"],
                event["asset"],
                event["side"],
                event["quantity"],
                event["price"],
                event["source_network"],
                parse_datetime(event["event_timestamp"]),
            ]
        ],
        column_names=[
            "event_id",
            "trade_id",
            "customer_id",
            "asset",
            "side",
            "quantity",
            "price",
            "source_network",
            "event_timestamp",
        ],
    )


def insert_reconciliation_result(client, event: dict[str, Any]) -> None:
    client.insert(
        "reconciliation_results",
        [
            [
                event["trade_id"],
                event["has_approved_order"],
                event["has_execution"],
                event.get("settlement_status"),
                event.get("custody_status"),
                event["is_complete"],
                event["reconciliation_status"],
            ]
        ],
        column_names=[
            "trade_id",
            "has_approved_order",
            "has_execution",
            "settlement_status",
            "custody_status",
            "is_complete",
            "reconciliation_status",
        ],
    )


def main() -> None:
    client = get_clickhouse_client()

    consumer.subscribe(
        [
            "approved.trade_orders",
            "reconciliation.results",
        ]
    )

    print("ClickHouse Writer started...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            event = json.loads(msg.value().decode("utf-8"))

            if topic == "approved.trade_orders":
                insert_approved_trade(client, event)
                print(f"CLICKHOUSE_INSERT approved_trade {event['trade_id']}")

            elif topic == "reconciliation.results":
                insert_reconciliation_result(client, event)
                print(f"CLICKHOUSE_INSERT reconciliation {event['trade_id']}")

    except KeyboardInterrupt:
        print("Stopping ClickHouse Writer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()