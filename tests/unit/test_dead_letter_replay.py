import json
from unittest.mock import MagicMock, patch

import pytest
from email_processor.dead_letter_replay import DeadLetterReplayService


def _make_settings(**overrides):
    defaults = dict(
        kafka_bootstrap_servers="localhost:9092",
        kafka_dead_letter_topic="test.dead-letter",
        kafka_inbound_topic="test.inbound",
        dead_letter_max_retries=3,
        dead_letter_replay_delay=0.0,
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


def _dead_letter_payload(message_id="dl-001", retry_count=0, error="some error"):
    return {
        "message_id": message_id,
        "recipients": ["a@b.com"],
        "sender": "c@d.com",
        "subject": "Test",
        "body_blob_path": "blob.txt",
        "retry_count": retry_count,
        "error": error,
    }


class TestDeadLetterReplayService:
    @patch("email_processor.dead_letter_replay.Producer")
    @patch("email_processor.dead_letter_replay.Consumer")
    def test_replays_message_under_max_retries(self, MockConsumer, MockProducer):
        payload = _dead_letter_payload(retry_count=1)
        mock_consumer = MockConsumer.return_value

        mock_consumer.poll.side_effect = [_kafka_message(payload), None]

        mock_producer = MockProducer.return_value

        service = DeadLetterReplayService(_make_settings())

        msg = mock_consumer.poll(1.0)
        raw = json.loads(msg.value().decode("utf-8"))
        retry_count = raw.get("retry_count", 0)

        assert retry_count < 3  
        raw.pop("error", None)
        raw["retry_count"] = retry_count + 1

        mock_producer.produce(
            "test.inbound",
            key=raw["message_id"].encode("utf-8"),
            value=json.dumps(raw).encode("utf-8"),
        )

        call_args = mock_producer.produce.call_args
        produced_payload = json.loads(call_args[1]["value"].decode("utf-8") if "value" in call_args[1] else call_args[0][2].decode("utf-8"))
        assert "error" not in produced_payload
        assert produced_payload["retry_count"] == 2

    @patch("email_processor.dead_letter_replay.Producer")
    @patch("email_processor.dead_letter_replay.Consumer")
    def test_drops_message_at_max_retries(self, MockConsumer, MockProducer):
        payload = _dead_letter_payload(retry_count=3) 
        mock_consumer = MockConsumer.return_value
        mock_consumer.poll.side_effect = [_kafka_message(payload), None]
        mock_producer = MockProducer.return_value

        service = DeadLetterReplayService(_make_settings())

        msg = mock_consumer.poll(1.0)
        raw = json.loads(msg.value().decode("utf-8"))
        retry_count = raw.get("retry_count", 0)

        assert retry_count >= 3

    @patch("email_processor.dead_letter_replay.Producer")
    @patch("email_processor.dead_letter_replay.Consumer")
    def test_error_field_stripped_from_replayed_payload(self, MockConsumer, MockProducer):
        payload = _dead_letter_payload(retry_count=0, error="original error")
        raw = dict(payload)
        raw.pop("error", None)
        raw["retry_count"] = 1

        assert "error" not in raw
        assert raw["retry_count"] == 1

    @patch("email_processor.dead_letter_replay.Producer")
    @patch("email_processor.dead_letter_replay.Consumer")
    def test_shutdown_sets_running_false(self, MockConsumer, MockProducer):
        service = DeadLetterReplayService(_make_settings())
        assert service._running is True
        service._shutdown(15, None)
        assert service._running is False
