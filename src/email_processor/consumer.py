import json
import logging

from confluent_kafka import Consumer, KafkaError

from config.settings import Settings
from email_processor.models import InboundEmailMessage

logger = logging.getLogger(__name__)


class EmailConsumer:
    def __init__(self, settings: Settings):
        logger.info(
            "Initializing Kafka consumer group=%s topic=%s servers=%s",
            settings.kafka_consumer_group,
            settings.kafka_inbound_topic,
            settings.kafka_bootstrap_servers,
        )
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_consumer_group,
                "auto.offset.reset": settings.kafka_auto_offset_reset,
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([settings.kafka_inbound_topic])

    def poll(self, timeout: float = 1.0) -> InboundEmailMessage | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            logger.error("Consumer error: %s", msg.error())
            return None
        try:
            payload = json.loads(msg.value().decode("utf-8"))
            return InboundEmailMessage(**payload)
        except Exception:
            logger.exception("Failed to deserialize message: %s", msg.value())
            return None

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)
        logger.debug("Offset committed")

    def close(self) -> None:
        logger.info("Closing Kafka consumer")
        self._consumer.close()
