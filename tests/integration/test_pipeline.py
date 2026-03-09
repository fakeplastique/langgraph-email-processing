import json
import uuid
from pathlib import Path
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from email_processor.service import EmailProcessorService


@pytest.fixture
def topic_names(unique_topic_prefix):
    """Unique topic names per test to prevent cross-test message pollution"""
    return {
        "kafka_inbound_topic": f"{unique_topic_prefix}-inbound",
        "kafka_summary_topic": f"{unique_topic_prefix}-summaries",
        "kafka_classification_topic": f"{unique_topic_prefix}-classifications",
        "kafka_dead_letter_topic": f"{unique_topic_prefix}-dead-letter",
    }


@pytest.fixture
def pipeline_settings(settings, topic_names, unique_group_id, tmp_path):
    """Settings with unique topics, group, and blob dir"""
    body = (
        "Dear Support,\n\nI would like to know the status of my order #12345.\n\n"
        "Thank you,\nJohn"
    )
    (tmp_path / "email_001.txt").write_text(body, encoding="utf-8")
    return settings.model_copy(update={
        **topic_names,
        "kafka_consumer_group": unique_group_id,
        "blob_storage_root": str(tmp_path),
    })


@pytest.fixture
def setup_pipeline_topics(settings, topic_names):
    """Create unique Kafka topics for this test"""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topic_names.values()]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
        except Exception:
            pass


def test_end_to_end(pipeline_settings, setup_pipeline_topics, pg_store, mock_llm):
    """Full pipeline: produce inbound → service processes → check DB + output topics"""
    producer = Producer({"bootstrap.servers": pipeline_settings.kafka_bootstrap_servers})
    msg_id = f"e2e-{uuid.uuid4().hex[:8]}"
    inbound_msg = {
        "message_id": msg_id,
        "recipients": ["support@example.com"],
        "sender": "john@example.com",
        "subject": "Order Status",
        "body_blob_path": "email_001.txt",
    }
    producer.produce(
        pipeline_settings.kafka_inbound_topic,
        key=msg_id.encode(),
        value=json.dumps(inbound_msg).encode("utf-8"),
    )
    producer.flush()

    service = EmailProcessorService(pipeline_settings, llm=mock_llm)
    project_root = Path(__file__).resolve().parents[2]
    service.pg_store.init_schema(str(project_root / "sql" / "init.sql"))

    msg = service.consumer.poll(timeout=15.0)
    assert msg is not None, "Did not receive inbound message from Kafka"
    service._process_message(msg)

    record = pg_store.get_by_message_id(msg_id)
    assert record is not None
    assert record.status == "completed"
    assert record.category == "inquiry"
    assert record.summary is not None

    summary_consumer = Consumer({
        "bootstrap.servers": pipeline_settings.kafka_bootstrap_servers,
        "group.id": f"test-sum-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    summary_consumer.subscribe([pipeline_settings.kafka_summary_topic])
    summary_msg = summary_consumer.poll(timeout=10.0)
    assert summary_msg is not None
    summary_data = json.loads(summary_msg.value().decode("utf-8"))
    assert summary_data["message_id"] == msg_id
    summary_consumer.close()

    classification_consumer = Consumer({
        "bootstrap.servers": pipeline_settings.kafka_bootstrap_servers,
        "group.id": f"test-cls-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    classification_consumer.subscribe([pipeline_settings.kafka_classification_topic])
    classification_msg = classification_consumer.poll(timeout=10.0)
    assert classification_msg is not None
    classification_data = json.loads(classification_msg.value().decode("utf-8"))
    assert classification_data["message_id"] == msg_id
    assert classification_data["category"] == "inquiry"
    classification_consumer.close()

    service.consumer.close()
    service.pg_store.close()


def test_idempotency(pipeline_settings, pg_store, mock_llm):
    """Processing the same message twice should not create duplicate DB records"""
    producer = Producer({"bootstrap.servers": pipeline_settings.kafka_bootstrap_servers})
    msg_id = f"idem-{uuid.uuid4().hex[:8]}"
    inbound_msg = {
        "message_id": msg_id,
        "recipients": ["support@example.com"],
        "sender": "john@example.com",
        "subject": "Duplicate Test",
        "body_blob_path": "email_001.txt",
    }

    for _ in range(2):
        producer.produce(
            pipeline_settings.kafka_inbound_topic,
            key=msg_id.encode(),
            value=json.dumps(inbound_msg).encode("utf-8"),
        )
    producer.flush()

    service = EmailProcessorService(pipeline_settings, llm=mock_llm)
    project_root = Path(__file__).resolve().parents[2]
    service.pg_store.init_schema(str(project_root / "sql" / "init.sql"))

    for _ in range(2):
        msg = service.consumer.poll(timeout=15.0)
        assert msg is not None
        service._process_message(msg)

    record = pg_store.get_by_message_id(msg_id)
    assert record is not None
    assert record.status == "completed"

    service.consumer.close()
    service.pg_store.close()


def test_failure_sends_to_dead_letter(pipeline_settings, setup_pipeline_topics, pg_store, mock_llm):
    """When blob is missing, message should be sent to dead-letter topic and DB status = failed"""
    producer = Producer({"bootstrap.servers": pipeline_settings.kafka_bootstrap_servers})
    msg_id = f"fail-{uuid.uuid4().hex[:8]}"
    inbound_msg = {
        "message_id": msg_id,
        "recipients": ["support@example.com"],
        "sender": "john@example.com",
        "subject": "Missing Blob Test",
        "body_blob_path": "nonexistent_file.txt",
    }
    producer.produce(
        pipeline_settings.kafka_inbound_topic,
        key=msg_id.encode(),
        value=json.dumps(inbound_msg).encode("utf-8"),
    )
    producer.flush()

    service = EmailProcessorService(pipeline_settings, llm=mock_llm)
    project_root = Path(__file__).resolve().parents[2]
    service.pg_store.init_schema(str(project_root / "sql" / "init.sql"))

    msg = service.consumer.poll(timeout=15.0)
    assert msg is not None
    service._process_message(msg)

    record = pg_store.get_by_message_id(msg_id)
    assert record is not None
    assert record.status == "failed"
    assert record.error_message is not None

    dl_consumer = Consumer({
        "bootstrap.servers": pipeline_settings.kafka_bootstrap_servers,
        "group.id": f"test-dl-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    dl_consumer.subscribe([pipeline_settings.kafka_dead_letter_topic])
    dl_msg = dl_consumer.poll(timeout=10.0)
    assert dl_msg is not None
    dl_data = json.loads(dl_msg.value().decode("utf-8"))
    assert dl_data["message_id"] == msg_id
    assert "error" in dl_data
    dl_consumer.close()

    service.consumer.close()
    service.pg_store.close()


def test_processing_with_retry_count_preserved(pipeline_settings, setup_pipeline_topics, pg_store, mock_llm):
    """A retried message (retry_count > 0) should still be processed normally"""
    producer = Producer({"bootstrap.servers": pipeline_settings.kafka_bootstrap_servers})
    msg_id = f"retry-{uuid.uuid4().hex[:8]}"
    inbound_msg = {
        "message_id": msg_id,
        "recipients": ["support@example.com"],
        "sender": "john@example.com",
        "subject": "Retry Test",
        "body_blob_path": "email_001.txt",
        "retry_count": 2,
    }
    producer.produce(
        pipeline_settings.kafka_inbound_topic,
        key=msg_id.encode(),
        value=json.dumps(inbound_msg).encode("utf-8"),
    )
    producer.flush()

    service = EmailProcessorService(pipeline_settings, llm=mock_llm)
    project_root = Path(__file__).resolve().parents[2]
    service.pg_store.init_schema(str(project_root / "sql" / "init.sql"))

    msg = service.consumer.poll(timeout=15.0)
    assert msg is not None
    service._process_message(msg)

    record = pg_store.get_by_message_id(msg_id)
    assert record is not None
    assert record.status == "completed"

    service.consumer.close()
    service.pg_store.close()
