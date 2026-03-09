import uuid

import pytest

from email_processor.models import EmailProcessingRecord
from email_processor.pg_store import PgStore


def _unique_id():
    return f"pg-{uuid.uuid4().hex[:8]}"


class TestPgStoreBasic:
    def test_upsert_and_get(self, pg_store):
        mid = _unique_id()
        record = EmailProcessingRecord(
            message_id=mid,
            sender="alice@example.com",
            recipients=["bob@example.com"],
            subject="Test Email",
            summary="A test summary",
            key_points=["point 1"],
            category="inquiry",
            confidence=0.95,
            labels=["test"],
            status="completed",
        )
        pg_store.upsert_result(record)

        fetched = pg_store.get_by_message_id(mid)
        assert fetched is not None
        assert fetched.message_id == mid
        assert fetched.sender == "alice@example.com"
        assert fetched.summary == "A test summary"
        assert fetched.category == "inquiry"
        assert fetched.status == "completed"

    def test_upsert_idempotency(self, pg_store):
        mid = _unique_id()
        record1 = EmailProcessingRecord(
            message_id=mid,
            sender="alice@example.com",
            recipients=["bob@example.com"],
            subject="Test Email",
            summary="First summary",
            category="inquiry",
            confidence=0.8,
            labels=["test"],
            status="completed",
        )
        pg_store.upsert_result(record1)

        record2 = EmailProcessingRecord(
            message_id=mid,
            sender="alice@example.com",
            recipients=["bob@example.com"],
            subject="Test Email",
            summary="Updated summary",
            category="complaint",
            confidence=0.9,
            labels=["updated"],
            status="completed",
        )
        pg_store.upsert_result(record2)

        fetched = pg_store.get_by_message_id(mid)
        assert fetched is not None
        assert fetched.summary == "Updated summary"
        assert fetched.category == "complaint"

    def test_get_nonexistent(self, pg_store):
        result = pg_store.get_by_message_id("nonexistent-id-xyz")
        assert result is None


class TestPgStoreAdvanced:
    def test_status_transitions(self, pg_store):
        """Record can transition from pending → failed → completed"""
        mid = _unique_id()

        pg_store.upsert_result(EmailProcessingRecord(
            message_id=mid,
            sender="a@b.com",
            recipients=["c@d.com"],
            subject="Status Test",
            status="pending",
        ))
        assert pg_store.get_by_message_id(mid).status == "pending"

        pg_store.upsert_result(EmailProcessingRecord(
            message_id=mid,
            sender="a@b.com",
            recipients=["c@d.com"],
            subject="Status Test",
            status="failed",
            error_message="blob not found",
        ))
        rec = pg_store.get_by_message_id(mid)
        assert rec.status == "failed"
        assert rec.error_message == "blob not found"

        pg_store.upsert_result(EmailProcessingRecord(
            message_id=mid,
            sender="a@b.com",
            recipients=["c@d.com"],
            subject="Status Test",
            summary="Done",
            category="order",
            confidence=0.95,
            labels=["order"],
            status="completed",
        ))
        rec = pg_store.get_by_message_id(mid)
        assert rec.status == "completed"
        assert rec.summary == "Done"

    def test_init_schema_idempotent(self, settings):
        """Calling init_schema twice should not raise"""
        from pathlib import Path
        project_root = Path(__file__).resolve().parents[2]

        store = PgStore(settings.pg_dsn)
        store.init_schema(str(project_root / "sql" / "init.sql"))
        store.init_schema(str(project_root / "sql" / "init.sql"))  # second call
        store.close()

    def test_multiple_records(self, pg_store):
        """Insert multiple distinct records and retrieve each"""
        ids = [_unique_id() for _ in range(5)]
        for i, mid in enumerate(ids):
            pg_store.upsert_result(EmailProcessingRecord(
                message_id=mid,
                sender=f"sender{i}@test.com",
                recipients=[f"recv{i}@test.com"],
                subject=f"Subject {i}",
                status="completed",
            ))

        for i, mid in enumerate(ids):
            rec = pg_store.get_by_message_id(mid)
            assert rec is not None
            assert rec.sender == f"sender{i}@test.com"
