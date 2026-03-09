from datetime import datetime

import pytest

from email_processor.models import (
    ClassificationResult,
    EmailProcessingRecord,
    InboundEmailMessage,
    SummaryResult,
)


class TestInboundEmailMessage:
    def test_defaults(self):
        msg = InboundEmailMessage(
            message_id="m1",
            recipients=["a@b.com"],
            sender="c@d.com",
            subject="Hi",
            body_blob_path="blob.txt",
        )
        assert msg.retry_count == 0
        assert isinstance(msg.received_at, datetime)

    def test_retry_count_preserved(self):
        msg = InboundEmailMessage(
            message_id="m2",
            recipients=["a@b.com"],
            sender="c@d.com",
            subject="Hi",
            body_blob_path="blob.txt",
            retry_count=3,
        )
        assert msg.retry_count == 3

    def test_roundtrip_json(self):
        msg = InboundEmailMessage(
            message_id="m3",
            recipients=["a@b.com", "x@y.com"],
            sender="c@d.com",
            subject="Test",
            body_blob_path="p.txt",
        )
        data = msg.model_dump_json()
        restored = InboundEmailMessage.model_validate_json(data)
        assert restored.message_id == msg.message_id
        assert restored.recipients == msg.recipients


class TestSummaryResult:
    def test_creation(self):
        r = SummaryResult(
            message_id="s1",
            summary="A summary",
            key_points=["a", "b"],
        )
        assert r.message_id == "s1"
        assert isinstance(r.processed_at, datetime)

    def test_serialization(self):
        r = SummaryResult(message_id="s2", summary="text", key_points=[])
        data = r.model_dump()
        assert "processed_at" in data
        assert data["key_points"] == []


class TestClassificationResult:
    def test_creation(self):
        r = ClassificationResult(
            message_id="c1",
            category="spam",
            confidence=0.99,
            labels=["spam", "marketing"],
        )
        assert r.category == "spam"
        assert r.confidence == 0.99

    def test_serialization(self):
        r = ClassificationResult(
            message_id="c2", category="order", confidence=0.5, labels=[]
        )
        data = r.model_dump()
        assert data["category"] == "order"


class TestEmailProcessingRecord:
    def test_defaults(self):
        r = EmailProcessingRecord(
            message_id="r1",
            sender="a@b.com",
            recipients=["c@d.com"],
            subject="Sub",
        )
        assert r.id is None
        assert r.status == "pending"
        assert r.summary is None
        assert r.key_points == []
        assert r.labels == []
        assert r.error_message is None

    def test_completed_record(self):
        r = EmailProcessingRecord(
            message_id="r2",
            sender="a@b.com",
            recipients=["c@d.com"],
            subject="Sub",
            summary="Done",
            key_points=["kp1"],
            category="inquiry",
            confidence=0.9,
            labels=["test"],
            status="completed",
        )
        assert r.status == "completed"
        assert r.confidence == 0.9
