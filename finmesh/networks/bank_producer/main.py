import json
import random
import time

from shared.kafka import create_producer

from shared.schemas.trade_order import TradeOrderCreated, TradeSide


producer = create_producer()

ASSETS = ["AAPL", "MSFT", "TSLA", "NVDA", "BTC"]
CUSTOMERS = [f"CUST-{i:03d}" for i in range(1, 51)]


def create_trade_order() -> TradeOrderCreated:
    quantity = random.randint(1, 100)
    price = round(random.uniform(50, 500), 2)
    asset = random.choice(ASSETS)

    # Intentionally create bad data sometimes
    if random.random() < 0.10:
        quantity = random.choice([0, -5])

    if random.random() < 0.10:
        price = random.choice([0, -100])

    if random.random() < 0.10:
        asset = "UNKNOWN"

    return TradeOrderCreated(
        trade_id=f"TRD-{random.randint(1000, 9999)}",
        customer_id=random.choice(CUSTOMERS),
        asset=asset,
        side=random.choice([TradeSide.BUY, TradeSide.SELL]),
        quantity=quantity,
        price=price,
    )

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced {msg.key().decode()} to {msg.topic()}")


def main():
    while True:
        event = create_trade_order()
        payload = event.model_dump()

        producer.produce(
            topic="raw.trade_orders",
            key=event.trade_id,
            value=json.dumps(payload),
            callback=delivery_report,
        )

        producer.poll(0)
        time.sleep(2)


if __name__ == "__main__":
    main()