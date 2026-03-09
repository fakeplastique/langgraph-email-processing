import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError

from email_processor.consumer import EmailConsumer
from email_processor.models import InboundEmailMessage


def _make_settings(**overrides):
    defaults = dict(
        kafka_bootstrap_servers="localhost:9092",
        kafka_consumer_group="test-group",
        kafka_inbound_topic="test.inbound",
        kafka_auto_offset_reset="earliest",
        kafka_commit_retry_max_attempts=2,
        kafka_commit_retry_wait=0.01,
    )
    defaults.update(overrides)
    s = MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


def _kafka_message(payload: dict):
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = json.dumps(payload).encode("utf-8")
    return msg


def _kafka_error_message(error_code):
    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = error_code
    msg.error.return_value = err
    return msg


class TestEmailConsumer:
    @patch("email_processor.consumer.Consumer")
    def test_poll_success(self, MockConsumer):
        payload = {
            "message_id": "m1",
            "recipients": ["a@b.com"],
            "sender": "c@d.com",
            "subject": "Hi",
            "body_blob_path": "blob.txt",
        }
        mock_instance = MockConsumer.return_value
        mock_instance.poll.return_value = _kafka_message(payload)

        consumer = EmailConsumer(_make_settings())
        result = consumer.poll(timeout=1.0)

        assert isinstance(result, InboundEmailMessage)
        assert result.message_id == "m1"
        assert result.sender == "c@d.com"

    @patch("email_processor.consumer.Consumer")
    def test_poll_returns_none_on_no_message(self, MockConsumer):
        mock_instance = MockConsumer.return_value
        mock_instance.poll.return_value = None

        consumer = EmailConsumer(_make_settings())
        assert consumer.poll() is None

    @patch("email_processor.consumer.Consumer")
    def test_poll_returns_none_on_partition_eof(self, MockConsumer):
        mock_instance = MockConsumer.return_value
        mock_instance.poll.return_value = _kafka_error_message(KafkaError._PARTITION_EOF)

        consumer = EmailConsumer(_make_settings())
        assert consumer.poll() is None

    @patch("email_processor.consumer.Consumer")
    def test_poll_returns_none_on_other_error(self, MockConsumer):
        mock_instance = MockConsumer.return_value
        mock_instance.poll.return_value = _kafka_error_message(KafkaError._TRANSPORT)

        consumer = EmailConsumer(_make_settings())
        assert consumer.poll() is None

    @patch("email_processor.consumer.Consumer")
    def test_poll_returns_none_on_deserialization_error(self, MockConsumer):
        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = b"not-valid-json"

        mock_instance = MockConsumer.return_value
        mock_instance.poll.return_value = msg

        consumer = EmailConsumer(_make_settings())
        assert consumer.poll() is None

    @patch("email_processor.consumer.Consumer")
    def test_commit_calls_underlying_consumer(self, MockConsumer):
        mock_instance = MockConsumer.return_value
        consumer = EmailConsumer(_make_settings())
        consumer.commit()
        mock_instance.commit.assert_called_once_with(asynchronous=False)

    @patch("email_processor.consumer.Consumer")
    def test_close(self, MockConsumer):
        mock_instance = MockConsumer.return_value
        consumer = EmailConsumer(_make_settings())
        consumer.close()
        mock_instance.close.assert_called_once()
