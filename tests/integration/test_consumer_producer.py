import json
import uuid

import pytest
from confluent_kafka import Producer, Consumer

from email_processor.consumer import EmailConsumer
from email_processor.models import (
    InboundEmailMessage,
    SummaryResult,
)
from email_processor.producer import EmailProducer


@pytest.fixture
def inbound_topic(create_topics, unique_topic_prefix):
    name = f"{unique_topic_prefix}-inbound"
    create_topics(name)
    return name


@pytest.fixture
def summary_topic(create_topics, unique_topic_prefix):
    name = f"{unique_topic_prefix}-summaries"
    create_topics(name)
    return name


@pytest.fixture
def classification_topic(create_topics, unique_topic_prefix):
    name = f"{unique_topic_prefix}-classifications"
    create_topics(name)
    return name


@pytest.fixture
def dead_letter_topic(create_topics, unique_topic_prefix):
    name = f"{unique_topic_prefix}-dead-letter"
    create_topics(name)
    return name


def test_produce_and_consume_roundtrip(settings, inbound_topic, unique_group_id):
    """Produce a message to Kafka, then consume it with EmailConsumer"""

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    payload = {
        "message_id": f"rt-{uuid.uuid4().hex[:8]}",
        "recipients": ["support@example.com"],
        "sender": "alice@example.com",
        "subject": "Roundtrip Test",
        "body_blob_path": "test.txt",
    }
    producer.produce(inbound_topic, key=payload["message_id"].encode(), value=json.dumps(payload).encode())
    producer.flush()

    consumer_settings = type(settings).model_construct(
        **{**settings.model_dump(), "kafka_inbound_topic": inbound_topic, "kafka_consumer_group": unique_group_id}
    )
    consumer = EmailConsumer(consumer_settings)
    try:
        msg = consumer.poll(timeout=15.0)
        assert msg is not None
        assert isinstance(msg, InboundEmailMessage)
        assert msg.message_id == payload["message_id"]
        assert msg.sender == "alice@example.com"
        consumer.commit()
    finally:
        consumer.close()


def test_producer_sends_summary_to_topic(settings, summary_topic, classification_topic, dead_letter_topic, unique_group_id):
    """EmailProducer.send_summary delivers to the correct topic"""
    producer_settings = type(settings).model_construct(
        **{
            **settings.model_dump(),
            "kafka_summary_topic": summary_topic,
            "kafka_classification_topic": classification_topic,
            "kafka_dead_letter_topic": dead_letter_topic,
        }
    )
    producer = EmailProducer(producer_settings)

    summary = SummaryResult(
        message_id="sum-int-001",
        summary="Integration test summary.",
        key_points=["point1", "point2"],
    )
    producer.send_summary(summary)
    producer.flush()

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": unique_group_id,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([summary_topic])
    try:
        msg = consumer.poll(15.0)
        assert msg is not None
        data = json.loads(msg.value().decode("utf-8"))
        assert data["message_id"] == "sum-int-001"
        assert data["summary"] == "Integration test summary."
    finally:
        consumer.close()


def test_producer_sends_dead_letter(settings, summary_topic, classification_topic, dead_letter_topic, unique_group_id):
    """EmailProducer.send_dead_letter delivers to the dead-letter topic with error info"""
    producer_settings = type(settings).model_construct(
        **{
            **settings.model_dump(),
            "kafka_summary_topic": summary_topic,
            "kafka_classification_topic": classification_topic,
            "kafka_dead_letter_topic": dead_letter_topic,
        }
    )
    producer = EmailProducer(producer_settings)

    msg = InboundEmailMessage(
        message_id="dl-int-001",
        recipients=["a@b.com"],
        sender="c@d.com",
        subject="Dead Letter Test",
        body_blob_path="x.txt",
        retry_count=2,
    )
    producer.send_dead_letter(msg, "processing failed")
    producer.flush()

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": unique_group_id,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([dead_letter_topic])
    try:
        raw = consumer.poll(15.0)
        assert raw is not None
        data = json.loads(raw.value().decode("utf-8"))
        assert data["message_id"] == "dl-int-001"
        assert data["error"] == "processing failed"
        assert data["retry_count"] == 2
    finally:
        consumer.close()
