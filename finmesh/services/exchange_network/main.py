import json
import random

from confluent_kafka import Consumer, Producer

from shared.schemas.trade_execution import (
    TradeExecutionCreated,
)


consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "exchange-network",
        "auto.offset.reset": "earliest",
    }
)

producer = Producer(
    {"bootstrap.servers": "localhost:9092"}
)


def main():

    consumer.subscribe(
        ["approved.trade_orders"]
    )

    print(
        "Exchange Network listening..."
    )

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            continue

        trade = json.loads(
            msg.value().decode()
        )

        execution_price = round(
            trade["price"]
            + random.uniform(-1.5, 1.5),
            2,
        )

        execution = (
            TradeExecutionCreated(
                execution_id=f"EXE-{random.randint(10000,99999)}",
                trade_id=trade["trade_id"],
                asset=trade["asset"],
                quantity=trade["quantity"],
                execution_price=execution_price,
            )
        )

        producer.produce(
            topic="exchange.trade_executions",
            key=trade["trade_id"],
            value=json.dumps(
                execution.model_dump()
            ),
        )

        producer.flush()

        print(
            f"EXECUTED {trade['trade_id']}"
        )


if __name__ == "__main__":
    main()