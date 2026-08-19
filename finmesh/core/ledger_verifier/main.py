import json
import logging

import psycopg2

from shared.config import settings
from shared.hashing import calculate_event_hash
from shared.logging_config import configure_logging


configure_logging()
logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
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
    conn = None

    try:
        conn = get_connection()
        rows = fetch_ledger_events(conn)

        if not rows:
            logger.warning("Ledger is empty")
            return

        expected_previous_hash = None

        for ledger_id, event_id, payload, previous_hash, event_hash in rows:
            if previous_hash != expected_previous_hash:
                logger.error(
                    "CHAIN_BROKEN ledger_id=%s event_id=%s "
                    "expected_previous_hash=%s actual_previous_hash=%s",
                    ledger_id,
                    event_id,
                    expected_previous_hash,
                    previous_hash,
                )
                return

            payload_dict = (
                payload
                if isinstance(payload, dict)
                else json.loads(payload)
            )

            recalculated_hash = calculate_event_hash(
                previous_hash,
                payload_dict,
            )

            if recalculated_hash != event_hash:
                logger.error(
                    "HASH_MISMATCH ledger_id=%s event_id=%s "
                    "expected_event_hash=%s actual_event_hash=%s",
                    ledger_id,
                    event_id,
                    recalculated_hash,
                    event_hash,
                )
                return

            expected_previous_hash = event_hash

        logger.info(
            "CHAIN_VALID verified_events=%s",
            len(rows),
        )

    except Exception:
        logger.exception("Ledger verification failed")
        raise

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()