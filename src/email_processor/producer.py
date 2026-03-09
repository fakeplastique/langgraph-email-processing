import logging

from confluent_kafka import KafkaException, Producer
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
    before_sleep_log,
)

from config.settings import Settings
from email_processor.models import ClassificationResult, SummaryResult

logger = logging.getLogger(__name__)


class EmailProducer:
    def __init__(self, settings: Settings):
        logger.info("Initializing Kafka producer servers=%s", settings.kafka_bootstrap_servers)
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "retries": settings.kafka_producer_retries,
                "retry.backoff.ms": settings.kafka_producer_retry_backoff_ms,
            }
        )
        self._summary_topic = settings.kafka_summary_topic
        self._classification_topic = settings.kafka_classification_topic
        self._flush_retry = retry(
            stop=stop_after_attempt(settings.kafka_flush_retry_max_attempts),
            wait=wait_fixed(1.0),
            retry=retry_if_exception_type((KafkaException, BufferError)),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    def _delivery_callback(self, err, msg):
        if err:
            logger.error("Delivery failed for %s: %s", msg.key(), err)
        else:
            logger.debug("Delivered to %s [%d]", msg.topic(), msg.partition())

    def send_summary(self, result: SummaryResult) -> None:
        logger.info("Producing summary for message %s to %s", result.message_id, self._summary_topic)
        self._producer.produce(
            self._summary_topic,
            key=result.message_id.encode("utf-8"),
            value=result.model_dump_json().encode("utf-8"),
            callback=self._delivery_callback,
        )

    def send_classification(self, result: ClassificationResult) -> None:
        logger.info("Producing classification for message %s to %s", result.message_id, self._classification_topic)
        self._producer.produce(
            self._classification_topic,
            key=result.message_id.encode("utf-8"),
            value=result.model_dump_json().encode("utf-8"),
            callback=self._delivery_callback,
        )

    def flush(self, timeout: float = 5.0) -> None:
        self._flush_retry(self._do_flush)(timeout)

    def _do_flush(self, timeout: float) -> None:
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            raise BufferError(f"{remaining} message(s) still in queue after flush timeout")
