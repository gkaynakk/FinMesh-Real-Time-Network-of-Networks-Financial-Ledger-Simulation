from confluent_kafka import Consumer, Producer

from shared.config import settings


def create_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
        }
    )


def create_consumer(group_id: str, auto_offset_reset: str = "earliest") -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
        }
    )