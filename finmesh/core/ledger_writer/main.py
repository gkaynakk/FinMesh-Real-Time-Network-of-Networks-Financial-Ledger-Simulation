import json
from typing import Any

import psycopg2
from confluent_kafka import Consumer

from shared.hashing import calculate_event_hash


consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "ledger-writer",
        "auto.offset.reset": "earliest",
    }
)


def get_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="finmesh",
        user="finmesh",
        password="finmesh",
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
    print(f"LEDGER_WRITTEN {event['trade_id']} hash={event_hash[:10]}...")


def main() -> None:
    consumer.subscribe(["approved.trade_orders"])
    conn = get_connection()

    print("Ledger writer started. Listening to approved.trade_orders...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            insert_ledger_event(conn, event)

    except KeyboardInterrupt:
        print("Stopping ledger writer...")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()