import json
import logging
from collections import defaultdict
from typing import Any

from shared.kafka import create_consumer, create_producer
from shared.logging_config import configure_logging


configure_logging()
logger = logging.getLogger(__name__)

consumer = create_consumer("reconciliation-service")
producer = create_producer()

trade_states: dict[str, dict[str, Any]] = defaultdict(dict)


def publish_result(trade_id: str, state: dict[str, Any]) -> None:
    result = {
        "trade_id": trade_id,
        "has_approved_order": state.get("approved_order", False),
        "has_execution": state.get("execution", False),
        "settlement_status": state.get("settlement_status"),
        "custody_status": state.get("custody_status"),
        "is_complete": (
            state.get("approved_order", False)
            and state.get("execution", False)
            and state.get("settlement_status") == "SETTLED"
            and state.get("custody_status") == "DELIVERED"
        ),
    }

    if result["is_complete"]:
        result["reconciliation_status"] = "CONSISTENT"
    elif state.get("settlement_status") == "FAILED":
        result["reconciliation_status"] = "SETTLEMENT_FAILED"
    elif state.get("custody_status") == "BLOCKED":
        result["reconciliation_status"] = "CUSTODY_BLOCKED"
    else:
        result["reconciliation_status"] = "PENDING"

    producer.produce(
        topic="reconciliation.results",
        key=trade_id,
        value=json.dumps(result),
    )
    producer.flush()

    status = result["reconciliation_status"]

    if status == "CONSISTENT":
        logger.info(
            "RECONCILED trade_id=%s status=%s",
            trade_id,
            status,
        )
    elif status in {"SETTLEMENT_FAILED", "CUSTODY_BLOCKED"}:
        logger.warning(
            "RECONCILED trade_id=%s status=%s",
            trade_id,
            status,
        )
    else:
        logger.info(
            "RECONCILED trade_id=%s status=%s",
            trade_id,
            status,
        )


def main():
    consumer.subscribe(
        [
            "approved.trade_orders",
            "exchange.trade_executions",
            "settlement.events",
            "custody.asset_movements",
        ]
    )

    logger.info(
        "Reconciliation Service started. Listening to lifecycle topics"
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
            trade_id = event["trade_id"]

            state = trade_states[trade_id]

            if topic == "approved.trade_orders":
                state["approved_order"] = True

            elif topic == "exchange.trade_executions":
                state["execution"] = True
                state["execution_id"] = event["execution_id"]

            elif topic == "settlement.events":
                state["settlement_status"] = event["status"]
                state["settlement_reason"] = event.get("reason")

            elif topic == "custody.asset_movements":
                state["custody_status"] = event["status"]
                state["custody_reason"] = event.get("reason")

            publish_result(trade_id, state)

    except KeyboardInterrupt:
        logger.info("Stopping Reconciliation Service")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()