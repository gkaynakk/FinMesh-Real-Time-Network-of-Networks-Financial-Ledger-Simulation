import json
import random

from shared.kafka import create_consumer, create_producer

from shared.schemas.custody_event import CustodyEventCreated, CustodyStatus

consumer = create_consumer("custody-network")
producer = create_producer()

BLOCK_REASONS = [
    "asset_freeze",
    "custody_account_mismatch",
    "manual_review_required",
]


def main() -> None:
    consumer.subscribe(["settlement.events"])

    print("Custody Network started. Listening to settlement.events...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            settlement = json.loads(msg.value().decode("utf-8"))

            # Custody only processes settled trades
            if settlement["status"] != "SETTLED":
                print(f"SKIPPED custody for failed settlement {settlement['trade_id']}")
                continue

            is_blocked = random.random() < 0.08

            status = CustodyStatus.BLOCKED if is_blocked else CustodyStatus.DELIVERED
            reason = random.choice(BLOCK_REASONS) if is_blocked else None

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

            print(
                f"CUSTODY {custody_event.status} "
                f"{custody_event.trade_id} reason={reason}"
            )

    except KeyboardInterrupt:
        print("Stopping Custody Network...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()