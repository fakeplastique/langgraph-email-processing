import json
from datetime import datetime, UTC
from unittest.mock import MagicMock, patch

import pytest

from email_processor.models import ClassificationResult, InboundEmailMessage, SummaryResult
from email_processor.producer import EmailProducer


def _make_settings(**overrides):
    defaults = dict(
        kafka_bootstrap_servers="localhost:9092",
        kafka_summary_topic="test.summaries",
        kafka_classification_topic="test.classifications",
        kafka_dead_letter_topic="test.dead-letter",
        kafka_producer_retries=1,
        kafka_producer_retry_backoff_ms=10,
        kafka_flush_retry_max_attempts=2,
    )
    defaults.update(overrides)
    s = MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


class TestEmailProducer:
    @patch("email_processor.producer.Producer")
    def test_send_summary(self, MockProducer):
        mock_instance = MockProducer.return_value
        producer = EmailProducer(_make_settings())

        result = SummaryResult(
            message_id="s1",
            summary="A summary",
            key_points=["p1"],
            processed_at=datetime.now(UTC),
        )
        producer.send_summary(result)

        mock_instance.produce.assert_called_once()
        args, kwargs = mock_instance.produce.call_args
        assert kwargs["topic"] if "topic" in kwargs else args[0] == "test.summaries"
        assert b"s1" in (kwargs.get("value") or args[1] if len(args) > 1 else kwargs["value"])

    @patch("email_processor.producer.Producer")
    def test_send_classification(self, MockProducer):
        mock_instance = MockProducer.return_value
        producer = EmailProducer(_make_settings())

        result = ClassificationResult(
            message_id="c1",
            category="spam",
            confidence=0.99,
            labels=["spam"],
            processed_at=datetime.now(UTC),
        )
        producer.send_classification(result)

        mock_instance.produce.assert_called_once()
        call_kwargs = mock_instance.produce.call_args
        
        assert call_kwargs[0][0] == "test.classifications"
        assert call_kwargs[1]["key"] == b"c1"

    @patch("email_processor.producer.Producer")
    def test_send_dead_letter(self, MockProducer):
        mock_instance = MockProducer.return_value
        producer = EmailProducer(_make_settings())

        msg = InboundEmailMessage(
            message_id="dl1",
            recipients=["a@b.com"],
            sender="c@d.com",
            subject="Fail",
            body_blob_path="x.txt",
            retry_count=1,
        )
        producer.send_dead_letter(msg, "some error")

        mock_instance.produce.assert_called_once()
        call_args = mock_instance.produce.call_args
        assert call_args[0][0] == "test.dead-letter"
        payload = json.loads(call_args[1]["value"].decode("utf-8"))
        assert payload["error"] == "some error"
        assert payload["retry_count"] == 1  

    @patch("email_processor.producer.Producer")
    def test_flush_success(self, MockProducer):
        mock_instance = MockProducer.return_value
        mock_instance.flush.return_value = 0
        producer = EmailProducer(_make_settings())
        producer.flush() 
        mock_instance.flush.assert_called()

    @patch("email_processor.producer.Producer")
    def test_flush_raises_on_remaining_messages(self, MockProducer):
        mock_instance = MockProducer.return_value
        mock_instance.flush.return_value = 5

        producer = EmailProducer(_make_settings(kafka_flush_retry_max_attempts=1))
        with pytest.raises(BufferError):
            producer.flush()
