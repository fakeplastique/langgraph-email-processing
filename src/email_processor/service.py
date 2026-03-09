import logging
import signal

from confluent_kafka.admin import AdminClient, NewTopic

from config.settings import Settings
from email_processor.agent.graph import build_graph
from email_processor.blob_store import LocalFileBlobStore
from email_processor.consumer import EmailConsumer
from email_processor.models import (
    ClassificationResult,
    EmailProcessingRecord,
    InboundEmailMessage,
    SummaryResult,
)
from email_processor.pg_store import PgStore
from email_processor.producer import EmailProducer

logger = logging.getLogger(__name__)


class EmailProcessorService:
    def __init__(self, settings: Settings, llm=None):
        self.settings = settings
        logger.info("Initializing EmailProcessorService")
        self.consumer = EmailConsumer(settings)
        self.producer = EmailProducer(settings)
        self.pg_store = PgStore(settings.pg_dsn)
        self.blob_store = LocalFileBlobStore(settings.blob_storage_root)
        self._running = True

        if llm is None:
            from langchain_anthropic import ChatAnthropic

            logger.info("Using ChatAnthropic with model=%s", settings.llm_model)
            llm = ChatAnthropic(
                model=settings.llm_model, api_key=settings.anthropic_api_key
            )

        self.graph = build_graph(self.blob_store, llm)
        logger.info("LangGraph agent built successfully")

    def _ensure_topics(self) -> None:
        admin = AdminClient(
            {"bootstrap.servers": self.settings.kafka_bootstrap_servers}
        )
        topics = [
            NewTopic(
                topic,
                num_partitions=self.settings.kafka_topic_partitions,
                replication_factor=1,
            )
            for topic in (
                self.settings.kafka_inbound_topic,
                self.settings.kafka_summary_topic,
                self.settings.kafka_classification_topic,
            )
        ]
        futures = admin.create_topics(topics)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info("Created topic %s", topic)
            except Exception:
                logger.debug("Topic %s already exists", topic)

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)
        self._ensure_topics()
        logger.info("Email processor service started")

        while self._running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                self._process_message(msg)
            except Exception:
                logger.exception("Error in main loop")

        self._cleanup()

    def _process_message(self, msg: InboundEmailMessage) -> None:
        logger.info("Processing message %s", msg.message_id)

        existing = self.pg_store.get_by_message_id(msg.message_id)
        if existing and existing.status == "completed":
            logger.info("Message %s already processed, skipping", msg.message_id)
            self.consumer.commit()
            return

        try:
            result = self.graph.invoke(
                {
                    "message_id": msg.message_id,
                    "sender": msg.sender,
                    "recipients": msg.recipients,
                    "subject": msg.subject,
                    "body_blob_path": msg.body_blob_path,
                    "body": "",
                    "classification": None,
                    "summary": None,
                    "error": None,
                }
            )

            if result.get("error"):
                self._handle_failure(msg, result["error"])
                return

            classification: ClassificationResult | None = result.get("classification")
            summary: SummaryResult | None = result.get("summary")

            record = EmailProcessingRecord(
                message_id=msg.message_id,
                sender=msg.sender,
                recipients=msg.recipients,
                subject=msg.subject,
                summary=summary.summary if summary else None,
                key_points=summary.key_points if summary else [],
                category=classification.category if classification else None,
                confidence=classification.confidence if classification else None,
                labels=classification.labels if classification else [],
                status="completed",
            )
            self.pg_store.upsert_result(record)

            if summary:
                self.producer.send_summary(summary)
            if classification:
                self.producer.send_classification(classification)
            self.producer.flush()

            self.consumer.commit()
            logger.info("Message %s processed successfully", msg.message_id)

        except Exception as e:
            logger.exception("Failed to process message %s", msg.message_id)
            self._handle_failure(msg, str(e))

    def _handle_failure(self, msg: InboundEmailMessage, error: str) -> None:
        logger.warning("Handling failure for message %s: %s", msg.message_id, error)
        record = EmailProcessingRecord(
            message_id=msg.message_id,
            sender=msg.sender,
            recipients=msg.recipients,
            subject=msg.subject,
            status="failed",
            error_message=error,
        )
        self.pg_store.upsert_result(record)
        self.consumer.commit()

    def _shutdown(self, signum, frame) -> None:
        logger.info("Shutting down (signal %s)", signum)
        self._running = False

    def _cleanup(self) -> None:
        self.consumer.close()
        self.pg_store.close()
        logger.info("Cleanup complete")
