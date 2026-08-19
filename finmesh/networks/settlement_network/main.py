import json
import logging
import random

from shared.kafka import create_consumer, create_producer
from shared.logging_config import configure_logging
from shared.schemas.settlement_event import (
    SettlementEventCreated,
    SettlementStatus,
)


configure_logging()
logger = logging.getLogger(__name__)

consumer = create_consumer("settlement-network")
producer = create_producer()

FAILURE_REASONS = [
    "insufficient_liquidity",
    "counterparty_timeout",
    "custody_mismatch",
    "compliance_hold",
]


def main():
    consumer.subscribe(["exchange.trade_executions"])

    logger.info(
        "Settlement Network started. Listening to exchange.trade_executions"
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            execution = json.loads(msg.value().decode("utf-8"))

            is_failed = random.random() < 0.12

            status = (
                SettlementStatus.FAILED
                if is_failed
                else SettlementStatus.SETTLED
            )

            reason = (
                random.choice(FAILURE_REASONS)
                if is_failed
                else None
            )

            settlement_event = SettlementEventCreated(
                settlement_id=f"SET-{random.randint(10000, 99999)}",
                execution_id=execution["execution_id"],
                trade_id=execution["trade_id"],
                status=status,
                reason=reason,
            )

            producer.produce(
                topic="settlement.events",
                key=execution["trade_id"],
                value=json.dumps(settlement_event.model_dump()),
            )

            producer.flush()

            if status == SettlementStatus.SETTLED:
                logger.info(
                    "SETTLED trade_id=%s settlement_id=%s",
                    execution["trade_id"],
                    settlement_event.settlement_id,
                )
            else:
                logger.warning(
                    "SETTLEMENT_FAILED trade_id=%s reason=%s",
                    execution["trade_id"],
                    reason,
                )

    except KeyboardInterrupt:
        logger.info("Stopping Settlement Network")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()