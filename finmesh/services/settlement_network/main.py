import json
import random

from confluent_kafka import Consumer, Producer

from shared.schemas.settlement_event import (
    SettlementEventCreated,
    SettlementStatus,
)


consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "settlement-network",
        "auto.offset.reset": "earliest",
    }
)

producer = Producer({"bootstrap.servers": "localhost:9092"})

FAILURE_REASONS = [
    "insufficient_liquidity",
    "counterparty_timeout",
    "custody_mismatch",
    "compliance_hold",
]


def main() -> None:
    consumer.subscribe(["exchange.trade_executions"])

    print("Settlement Network started. Listening to exchange.trade_executions...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            execution = json.loads(msg.value().decode("utf-8"))

            is_failed = random.random() < 0.12

            status = SettlementStatus.FAILED if is_failed else SettlementStatus.SETTLED
            reason = random.choice(FAILURE_REASONS) if is_failed else None

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

            print(
                f"SETTLEMENT {settlement_event.status} "
                f"{settlement_event.trade_id} reason={reason}"
            )

    except KeyboardInterrupt:
        print("Stopping Settlement Network...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()