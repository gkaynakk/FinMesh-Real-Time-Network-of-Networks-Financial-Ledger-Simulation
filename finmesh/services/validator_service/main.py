import json
from typing import Any

from confluent_kafka import Consumer, Producer

from shared.schemas.trade_order import TradeOrderCreated


consumer = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "validator-service",
        "auto.offset.reset": "earliest",
    }
)

producer = Producer({"bootstrap.servers": "localhost:9092"})

ALLOWED_ASSETS = {"AAPL", "MSFT", "TSLA", "NVDA", "BTC"}


def validate_trade_order(event: dict[str, Any]) -> tuple[bool, str | None]:
    try:
        trade = TradeOrderCreated(**event)
    except Exception as e:
        return False, f"schema_validation_failed: {str(e)}"

    if not trade.trade_id:
        return False, "missing_trade_id"

    if trade.quantity <= 0:
        return False, "quantity_must_be_positive"

    if trade.price <= 0:
        return False, "price_must_be_positive"

    if trade.asset not in ALLOWED_ASSETS:
        return False, "asset_not_allowed"

    return True, None


def publish(topic: str, key: str, value: dict[str, Any]) -> None:
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(value),
    )
    producer.flush()


def main() -> None:
    consumer.subscribe(["raw.trade_orders"])
    print("Validator service started. Listening to raw.trade_orders...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode("utf-8"))
            is_valid, rejection_reason = validate_trade_order(event)

            if is_valid:
                event["validation_status"] = "APPROVED"
                publish("approved.trade_orders", event["trade_id"], event)
                print(f"APPROVED {event['trade_id']}")
            else:
                event["validation_status"] = "REJECTED"
                event["rejection_reason"] = rejection_reason
                publish("rejected.trade_orders", event.get("trade_id", "unknown"), event)
                print(f"REJECTED {event.get('trade_id')} - {rejection_reason}")

    except KeyboardInterrupt:
        print("Stopping validator service...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()