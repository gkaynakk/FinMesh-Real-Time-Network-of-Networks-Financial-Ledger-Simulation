import json
import logging
import random

from shared.kafka import create_consumer, create_producer
from shared.logging_config import configure_logging
from shared.schemas.trade_execution import TradeExecutionCreated


configure_logging()
logger = logging.getLogger(__name__)

consumer = create_consumer("exchange-network")
producer = create_producer()


def main():
    consumer.subscribe(["approved.trade_orders"])

    logger.info(
        "Exchange Network started. Listening to approved.trade_orders"
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            trade = json.loads(msg.value().decode("utf-8"))

            execution_price = round(
                trade["price"] + random.uniform(-1.5, 1.5),
                2,
            )

            execution = TradeExecutionCreated(
                execution_id=f"EXE-{random.randint(10000, 99999)}",
                trade_id=trade["trade_id"],
                asset=trade["asset"],
                quantity=trade["quantity"],
                execution_price=execution_price,
            )

            producer.produce(
                topic="exchange.trade_executions",
                key=trade["trade_id"],
                value=json.dumps(execution.model_dump()),
            )

            producer.flush()

            logger.info(
                "EXECUTED trade_id=%s asset=%s quantity=%s price=%s",
                trade["trade_id"],
                trade["asset"],
                trade["quantity"],
                execution_price,
            )

    except KeyboardInterrupt:
        logger.info("Stopping Exchange Network")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()