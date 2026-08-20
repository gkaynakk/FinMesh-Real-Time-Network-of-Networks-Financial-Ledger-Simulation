import json
import logging
import random

from shared.kafka import create_consumer, create_producer
from shared.logging_config import configure_logging
from shared.schemas.custody_event import (
    CustodyEventCreated,
    CustodyStatus,
)


configure_logging()
logger = logging.getLogger(__name__)

consumer = create_consumer("custody-network")
producer = create_producer()

BLOCK_REASONS = [
    "asset_freeze",
    "custody_account_mismatch",
    "manual_review_required",
]


def main():
    consumer.subscribe(["settlement.events"])

    logger.info(
        "Custody Network started. Listening to settlement.events"
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            settlement = json.loads(msg.value().decode("utf-8"))

            # Custody only processes successfully settled trades.
            if settlement["status"] != "SETTLED":
                logger.info(
                    "SKIPPED custody trade_id=%s settlement_status=%s",
                    settlement["trade_id"],
                    settlement["status"],
                )
                continue

            is_blocked = random.random() < 0.08

            status = (
                CustodyStatus.BLOCKED
                if is_blocked
                else CustodyStatus.DELIVERED
            )

            reason = (
                random.choice(BLOCK_REASONS)
                if is_blocked
                else None
            )

            custody_event = CustodyEventCreated(
                custody_event_id=f"CUS-{random.randint(10000, 99999)}",
                settlement_id=settlement["settlement_id"],
                trade_id=settlement["trade_id"],
                status=status,
                reason=reason,
            )

            producer.produce(
                topic="custody.asset_movements",
                key=settlement["trade_id"],
                value=json.dumps(custody_event.model_dump()),
            )

            producer.flush()

            if status == CustodyStatus.DELIVERED:
                logger.info(
                    "CUSTODY_DELIVERED trade_id=%s custody_event_id=%s",
                    settlement["trade_id"],
                    custody_event.custody_event_id,
                )
            else:
                logger.warning(
                    "CUSTODY_BLOCKED trade_id=%s reason=%s",
                    settlement["trade_id"],
                    reason,
                )

    except KeyboardInterrupt:
        logger.info("Stopping Custody Network")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()