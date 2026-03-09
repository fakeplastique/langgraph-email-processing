import json
import logging

from confluent_kafka import Consumer, KafkaError, KafkaException
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
    before_sleep_log,
)

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
        self._commit_retry = retry(
            stop=stop_after_attempt(settings.kafka_commit_retry_max_attempts),
            wait=wait_fixed(settings.kafka_commit_retry_wait),
            retry=retry_if_exception_type(KafkaException),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

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
        self._commit_retry(self._do_commit)()

    def _do_commit(self) -> None:
        self._consumer.commit(asynchronous=False)
        logger.debug("Offset committed")

    def close(self) -> None:
        logger.info("Closing Kafka consumer")
        self._consumer.close()
