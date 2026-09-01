import logging
from datetime import datetime, timezone

from elasticsearch import Elasticsearch

from shared.kafka import create_consumer
from shared.logging_config import configure_logging


configure_logging()
logger = logging.getLogger(__name__)

INDEX_NAME = "finmesh-events"

TOPICS = [
    "approved.trade_orders",
    "exchange.trade_executions",
    "settlement.events",
    "custody.asset_movements",
    "reconciliation.results.v2",
]


def create_elasticsearch_client() -> Elasticsearch:
    return Elasticsearch("http://localhost:9200")


def build_document(topic: str, event: dict) -> dict:
    return {
        "trade_id": event.get("trade_id"),
        "topic": topic,
        "event_type": topic,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": event,
    }


def main():
    consumer = create_consumer("elasticsearch-writer")
    es = create_elasticsearch_client()

    consumer.subscribe(TOPICS)

    logger.info(
        "Elasticsearch Writer started. index=%s",
        INDEX_NAME,
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

            import json

            event = json.loads(msg.value().decode("utf-8"))
            document = build_document(topic, event)

            es.index(
                index=INDEX_NAME,
                document=document,
            )

            logger.info(
                "INDEXED trade_id=%s topic=%s",
                document["trade_id"],
                topic,
            )

    except KeyboardInterrupt:
        logger.info("Stopping Elasticsearch Writer")

    finally:
        consumer.close()
        es.close()


if __name__ == "__main__":
    main()
