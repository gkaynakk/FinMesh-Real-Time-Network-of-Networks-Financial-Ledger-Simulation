import json
import logging
from datetime import datetime
from typing import Any

import clickhouse_connect

from shared.config import settings
from shared.kafka import create_consumer
from shared.logging_config import configure_logging


configure_logging()
logger = logging.getLogger(__name__)

consumer = create_consumer("clickhouse-writer")


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
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

    logger.info(
        "ClickHouse Writer started. Listening to approved.trade_orders and reconciliation.results"
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            topic = msg.topic()
            event = json.loads(msg.value().decode("utf-8"))

            if topic == "approved.trade_orders":
                insert_approved_trade(client, event)

                logger.info(
                    "CLICKHOUSE_INSERT approved_trade trade_id=%s asset=%s",
                    event["trade_id"],
                    event["asset"],
                )

            elif topic == "reconciliation.results":
                insert_reconciliation_result(client, event)

                logger.info(
                    "CLICKHOUSE_INSERT reconciliation trade_id=%s status=%s",
                    event["trade_id"],
                    event["reconciliation_status"],
                )

    except KeyboardInterrupt:
        logger.info("Stopping ClickHouse Writer")

    except Exception:
        logger.exception("Unexpected ClickHouse Writer failure")
        raise

    finally:
        consumer.close()


if __name__ == "__main__":
    main()