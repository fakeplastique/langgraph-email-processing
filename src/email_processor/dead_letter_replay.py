import json
import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, Producer

from config.settings import Settings

logger = logging.getLogger(__name__)


class DeadLetterReplayService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._running = True
        self._max_retries = settings.dead_letter_max_retries
        self._replay_delay = settings.dead_letter_replay_delay

        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "dead-letter-replay",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([settings.kafka_dead_letter_topic])

        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        self._inbound_topic = settings.kafka_inbound_topic

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)
        logger.info("Dead letter replay service started (max_retries=%d, delay=%.1fs)", self._max_retries, self._replay_delay)

        while self._running:
            msg = self._consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                logger.exception("Failed to deserialize dead letter: %s", msg.value())
                self._consumer.commit(asynchronous=False)
                continue

            retry_count = payload.get("retry_count", 0)
            message_id = payload.get("message_id", "unknown")

            if retry_count >= self._max_retries:
                logger.error(
                    "Message %s exceeded max retries (%d/%d), dropping",
                    message_id, retry_count, self._max_retries,
                )
                self._consumer.commit(asynchronous=False)
                continue

            payload.pop("error", None)
            payload["retry_count"] = retry_count + 1

            logger.info(
                "Replaying message %s (retry %d/%d) after %.1fs delay",
                message_id, retry_count + 1, self._max_retries, self._replay_delay,
            )
            time.sleep(self._replay_delay)

            self._producer.produce(
                self._inbound_topic,
                key=message_id.encode("utf-8"),
                value=json.dumps(payload, default=str).encode("utf-8"),
            )
            self._producer.flush(5.0)
            self._consumer.commit(asynchronous=False)

        self._consumer.close()
        logger.info("Dead letter replay service stopped")

    def _shutdown(self, signum, frame) -> None:
        logger.info("Shutting down (signal %s)", signum)
        self._running = False


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    settings = Settings()
    service = DeadLetterReplayService(settings)
    service.run()


if __name__ == "__main__":
    main()
