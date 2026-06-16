import json
import psycopg2

from shared.hashing import calculate_event_hash


def get_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="finmesh",
        user="finmesh",
        password="finmesh",
    )


def fetch_ledger_events(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ledger_id,
                event_id,
                payload,
                previous_hash,
                event_hash
            FROM ledger_events
            ORDER BY ledger_id ASC;
            """
        )
        return cur.fetchall()


def main():
    conn = get_connection()
    rows = fetch_ledger_events(conn)

    if not rows:
        print("Ledger is empty.")
        return

    expected_previous_hash = None

    for ledger_id, event_id, payload, previous_hash, event_hash in rows:
        if previous_hash != expected_previous_hash:
            print(f"CHAIN BROKEN at ledger_id={ledger_id}")
            print(f"Expected previous_hash: {expected_previous_hash}")
            print(f"Actual previous_hash:   {previous_hash}")
            return

        payload_dict = payload if isinstance(payload, dict) else json.loads(payload)

        recalculated_hash = calculate_event_hash(previous_hash, payload_dict)

        if recalculated_hash != event_hash:
            print(f"HASH MISMATCH at ledger_id={ledger_id}")
            print(f"event_id: {event_id}")
            print(f"Expected event_hash: {recalculated_hash}")
            print(f"Actual event_hash:   {event_hash}")
            return

        expected_previous_hash = event_hash

    print(f"CHAIN VALID. Verified {len(rows)} ledger events.")

    conn.close()


if __name__ == "__main__":
    main()