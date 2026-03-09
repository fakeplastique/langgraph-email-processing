"""Unit tests for EmailProcessorService with mocked dependencies"""

from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from email_processor.models import (
    EmailProcessingRecord,
    InboundEmailMessage,
    ClassificationResult,
    SummaryResult,
)
from email_processor.agent.nodes import ClassificationOutput, SummaryOutput


def _make_msg(message_id="svc-test-001", **overrides):
    defaults = dict(
        message_id=message_id,
        recipients=["support@example.com"],
        sender="john@example.com",
        subject="Test Subject",
        body_blob_path="email.txt",
    )
    defaults.update(overrides)
    return InboundEmailMessage(**defaults)


def _graph_result(message_id="svc-test-001", error=None):
    if error:
        return {
            "message_id": message_id,
            "body": "",
            "classification": None,
            "summary": None,
            "error": error,
        }
    return {
        "message_id": message_id,
        "body": "email body",
        "classification": ClassificationResult(
            message_id=message_id, category="inquiry", confidence=0.9, labels=["order"]
        ),
        "summary": SummaryResult(
            message_id=message_id, summary="A summary.", key_points=["kp1"]
        ),
        "error": None,
    }


class TestProcessMessage:
    @patch("email_processor.service.EmailConsumer")
    @patch("email_processor.service.EmailProducer")
    @patch("email_processor.service.PgStore")
    @patch("email_processor.service.build_graph")
    def test_happy_path(self, mock_build_graph, MockPgStore, MockProducer, MockConsumer):
        from email_processor.service import EmailProcessorService

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = _graph_result()
        mock_build_graph.return_value = mock_graph

        mock_pg = MockPgStore.return_value
        mock_pg.get_by_message_id.return_value = None

        settings = MagicMock()
        settings.blob_storage_root = "/tmp"
        settings.pg_retry_max_attempts = 1
        settings.pg_retry_wait = 0.01
        settings.llm_retry_max_attempts = 1
        settings.llm_retry_initial_wait = 0.01
        settings.llm_retry_max_wait = 0.1
        settings.llm_retry_jitter = 0.0

        service = EmailProcessorService(settings, llm=MagicMock())
        service.pg_store = mock_pg
        service.graph = mock_graph

        msg = _make_msg()
        service._process_message(msg)

        mock_pg.upsert_result.assert_called_once()
        record = mock_pg.upsert_result.call_args[0][0]
        assert record.status == "completed"
        assert record.category == "inquiry"

        service.producer.send_summary.assert_called_once()
        service.producer.send_classification.assert_called_once()
        service.producer.flush.assert_called_once()
        service.consumer.commit.assert_called_once()

    @patch("email_processor.service.EmailConsumer")
    @patch("email_processor.service.EmailProducer")
    @patch("email_processor.service.PgStore")
    @patch("email_processor.service.build_graph")
    def test_skips_already_completed(self, mock_build_graph, MockPgStore, MockProducer, MockConsumer):
        from email_processor.service import EmailProcessorService

        mock_pg = MockPgStore.return_value
        mock_pg.get_by_message_id.return_value = EmailProcessingRecord(
            message_id="svc-test-001",
            sender="john@example.com",
            recipients=["support@example.com"],
            subject="Test",
            status="completed",
        )

        settings = MagicMock()
        settings.blob_storage_root = "/tmp"
        settings.pg_retry_max_attempts = 1
        settings.pg_retry_wait = 0.01
        settings.llm_retry_max_attempts = 1
        settings.llm_retry_initial_wait = 0.01
        settings.llm_retry_max_wait = 0.1
        settings.llm_retry_jitter = 0.0

        service = EmailProcessorService(settings, llm=MagicMock())
        service.pg_store = mock_pg
        service.graph = MagicMock()

        msg = _make_msg()
        service._process_message(msg)

        service.graph.invoke.assert_not_called()
        service.consumer.commit.assert_called_once()

    @patch("email_processor.service.EmailConsumer")
    @patch("email_processor.service.EmailProducer")
    @patch("email_processor.service.PgStore")
    @patch("email_processor.service.build_graph")
    def test_graph_error_triggers_dead_letter(self, mock_build_graph, MockPgStore, MockProducer, MockConsumer):
        from email_processor.service import EmailProcessorService

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = _graph_result(error="blob load failed")
        mock_build_graph.return_value = mock_graph

        mock_pg = MockPgStore.return_value
        mock_pg.get_by_message_id.return_value = None

        settings = MagicMock()
        settings.blob_storage_root = "/tmp"
        settings.pg_retry_max_attempts = 1
        settings.pg_retry_wait = 0.01
        settings.llm_retry_max_attempts = 1
        settings.llm_retry_initial_wait = 0.01
        settings.llm_retry_max_wait = 0.1
        settings.llm_retry_jitter = 0.0

        service = EmailProcessorService(settings, llm=MagicMock())
        service.pg_store = mock_pg
        service.graph = mock_graph

        msg = _make_msg()
        service._process_message(msg)

        # Should have called _handle_failure path
        record = mock_pg.upsert_result.call_args[0][0]
        assert record.status == "failed"
        service.producer.send_dead_letter.assert_called_once()

    @patch("email_processor.service.EmailConsumer")
    @patch("email_processor.service.EmailProducer")
    @patch("email_processor.service.PgStore")
    @patch("email_processor.service.build_graph")
    def test_exception_triggers_handle_failure(self, mock_build_graph, MockPgStore, MockProducer, MockConsumer):
        from email_processor.service import EmailProcessorService

        mock_graph = MagicMock()
        mock_graph.invoke.side_effect = RuntimeError("unexpected crash")
        mock_build_graph.return_value = mock_graph

        mock_pg = MockPgStore.return_value
        mock_pg.get_by_message_id.return_value = None

        settings = MagicMock()
        settings.blob_storage_root = "/tmp"
        settings.pg_retry_max_attempts = 1
        settings.pg_retry_wait = 0.01
        settings.llm_retry_max_attempts = 1
        settings.llm_retry_initial_wait = 0.01
        settings.llm_retry_max_wait = 0.1
        settings.llm_retry_jitter = 0.0

        service = EmailProcessorService(settings, llm=MagicMock())
        service.pg_store = mock_pg
        service.graph = mock_graph

        msg = _make_msg()
        service._process_message(msg)  # should not raise

        record = mock_pg.upsert_result.call_args[0][0]
        assert record.status == "failed"
        assert "unexpected crash" in record.error_message
        service.producer.send_dead_letter.assert_called_once()

    @patch("email_processor.service.EmailConsumer")
    @patch("email_processor.service.EmailProducer")
    @patch("email_processor.service.PgStore")
    @patch("email_processor.service.build_graph")
    def test_handle_failure_itself_fails_gracefully(self, mock_build_graph, MockPgStore, MockProducer, MockConsumer):
        from email_processor.service import EmailProcessorService

        mock_graph = MagicMock()
        mock_graph.invoke.side_effect = RuntimeError("crash")
        mock_build_graph.return_value = mock_graph

        mock_pg = MockPgStore.return_value
        mock_pg.get_by_message_id.return_value = None
        mock_pg.upsert_result.side_effect = Exception("DB down")

        settings = MagicMock()
        settings.blob_storage_root = "/tmp"
        settings.pg_retry_max_attempts = 1
        settings.pg_retry_wait = 0.01
        settings.llm_retry_max_attempts = 1
        settings.llm_retry_initial_wait = 0.01
        settings.llm_retry_max_wait = 0.1
        settings.llm_retry_jitter = 0.0

        service = EmailProcessorService(settings, llm=MagicMock())
        service.pg_store = mock_pg
        service.graph = mock_graph

        msg = _make_msg()
        service._process_message(msg)

    @patch("email_processor.service.EmailConsumer")
    @patch("email_processor.service.EmailProducer")
    @patch("email_processor.service.PgStore")
    @patch("email_processor.service.build_graph")
    @patch("email_processor.service.AdminClient")
    def test_ensure_topics(self, MockAdmin, mock_build_graph, MockPgStore, MockProducer, MockConsumer):
        from email_processor.service import EmailProcessorService

        mock_admin = MockAdmin.return_value
        future = MagicMock()
        future.result.return_value = None
        mock_admin.create_topics.return_value = {"topic1": future}

        settings = MagicMock()
        settings.blob_storage_root = "/tmp"
        settings.kafka_bootstrap_servers = "localhost:9092"
        settings.kafka_inbound_topic = "t1"
        settings.kafka_summary_topic = "t2"
        settings.kafka_classification_topic = "t3"
        settings.kafka_dead_letter_topic = "t4"
        settings.kafka_topic_partitions = 1
        settings.pg_retry_max_attempts = 1
        settings.pg_retry_wait = 0.01
        settings.llm_retry_max_attempts = 1
        settings.llm_retry_initial_wait = 0.01
        settings.llm_retry_max_wait = 0.1
        settings.llm_retry_jitter = 0.0

        service = EmailProcessorService(settings, llm=MagicMock())
        service._ensure_topics()

        MockAdmin.assert_called_with({"bootstrap.servers": "localhost:9092"})
        mock_admin.create_topics.assert_called_once()
