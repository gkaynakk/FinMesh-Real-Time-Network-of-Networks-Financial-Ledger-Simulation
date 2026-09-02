import json
import time
from uuid import uuid4

from shared.kafka import create_producer
from shared.schemas.trade_order import TradeOrderCreated, TradeSide


def main():
    producer = create_producer()

    trade_id = f"TRD-DEMO-{uuid4().hex[:6].upper()}"

    event = TradeOrderCreated(
        trade_id=trade_id,
        customer_id="CUST-DEMO",
        asset="MSFT",
        side=TradeSide.BUY,
        quantity=25,
        price=250.00,
    )

    payload = event.model_dump()

    print(f"Submitting demo trade: {trade_id}")

    producer.produce(
        topic="raw.trade_orders",
        key=trade_id,
        value=json.dumps(payload),
    )

    producer.flush()

    print("Trade submitted to raw.trade_orders.")
    print("Waiting for FinMesh lifecycle processing...")
    time.sleep(8)

    print()
    print(f"Demo trade ID: {trade_id}")
    print()
    print("Next:")
    print(f"  uv run python -m intelligence.main")
    print(f"  FinMesh> What happened to {trade_id}?")


if __name__ == "__main__":
    main()
