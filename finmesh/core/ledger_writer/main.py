import json
import logging
from typing import Any

import psycopg2

from shared.config import settings
from shared.hashing import calculate_event_hash
from shared.kafka import create_consumer
from shared.logging_config import configure_logging


configure_logging()
logger = logging.getLogger(__name__)

consumer = create_consumer("ledger-writer")


def get_connection():
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )


def get_latest_hash(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_hash
            FROM ledger_events
            ORDER BY ledger_id DESC
            LIMIT 1;
            """
        )

        row = cur.fetchone()
        return row[0] if row else None


def insert_ledger_event(conn, event: dict[str, Any]) -> None:
    previous_hash = get_latest_hash(conn)
    event_hash = calculate_event_hash(previous_hash, event)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ledger_events (
                event_id,
                event_type,
                source_network,
                payload,
                previous_hash,
                event_hash,
                validation_status,
                rejection_reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
            """,
            (
                event["event_id"],
                event["event_type"],
                event["source_network"],
                json.dumps(event),
                previous_hash,
                event_hash,
                event["validation_status"],
                event.get("rejection_reason"),
            ),
        )

    conn.commit()

    logger.info(
        "LEDGER_WRITTEN trade_id=%s event_id=%s hash=%s",
        event["trade_id"],
        event["event_id"],
        event_hash[:12],
    )


def main() -> None:
    consumer.subscribe(["approved.trade_orders"])

    logger.info(
        "Ledger Writer started. Listening to approved.trade_orders"
    )

    conn = None

    try:
        conn = get_connection()

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            event = json.loads(msg.value().decode("utf-8"))

            insert_ledger_event(conn, event)

    except KeyboardInterrupt:
        logger.info("Stopping Ledger Writer")

    except Exception:
        logger.exception("Unexpected Ledger Writer failure")

        if conn is not None:
            conn.rollback()

        raise

    finally:
        consumer.close()

        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()