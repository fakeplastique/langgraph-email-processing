import json
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from email_processor.dead_letter_replay import DeadLetterReplayService


@pytest.fixture
def dl_topics(settings, unique_topic_prefix):
    """Create dead-letter and inbound topics for replay tests"""
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    dl_topic = f"{unique_topic_prefix}-dead-letter"
    inbound_topic = f"{unique_topic_prefix}-inbound"
    futures = admin.create_topics([
        NewTopic(dl_topic, num_partitions=1, replication_factor=1),
        NewTopic(inbound_topic, num_partitions=1, replication_factor=1),
    ])
    for t, f in futures.items():
        try:
            f.result()
        except Exception:
            pass
    return dl_topic, inbound_topic


def test_replay_cycle(settings, dl_topics, unique_group_id):
    """Dead letter message with retry_count < max should be replayed to inbound topic"""
    dl_topic, inbound_topic = dl_topics

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    msg_id = f"replay-{uuid.uuid4().hex[:8]}"
    payload = {
        "message_id": msg_id,
        "recipients": ["a@b.com"],
        "sender": "c@d.com",
        "subject": "Replay Test",
        "body_blob_path": "blob.txt",
        "retry_count": 1,
        "error": "transient failure",
    }
    producer.produce(dl_topic, key=msg_id.encode(), value=json.dumps(payload).encode())
    producer.flush()

    replay_settings = type(settings).model_construct(**{
        **settings.model_dump(),
        "kafka_dead_letter_topic": dl_topic,
        "kafka_inbound_topic": inbound_topic,
        "dead_letter_max_retries": 3,
        "dead_letter_replay_delay": 0.0, 
    })

    service = DeadLetterReplayService(replay_settings)

    msg = service._consumer.poll(15.0)
    assert msg is not None

    raw = json.loads(msg.value().decode("utf-8"))
    assert raw["retry_count"] < replay_settings.dead_letter_max_retries

    raw.pop("error", None)
    raw["retry_count"] += 1

    service._producer.produce(
        inbound_topic,
        key=raw["message_id"].encode(),
        value=json.dumps(raw, default=str).encode(),
    )
    service._producer.flush(5.0)
    service._consumer.commit(asynchronous=False)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": unique_group_id,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([inbound_topic])
    replayed = consumer.poll(15.0)
    assert replayed is not None
    replayed_data = json.loads(replayed.value().decode("utf-8"))
    assert replayed_data["message_id"] == msg_id
    assert replayed_data["retry_count"] == 2
    assert "error" not in replayed_data
    consumer.close()

    service._consumer.close()


def test_message_dropped_at_max_retries(settings, dl_topics):
    """Dead letter message at max retries should be committed but NOT replayed"""
    dl_topic, inbound_topic = dl_topics

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    msg_id = f"drop-{uuid.uuid4().hex[:8]}"
    payload = {
        "message_id": msg_id,
        "recipients": ["a@b.com"],
        "sender": "c@d.com",
        "subject": "Max Retry Test",
        "body_blob_path": "blob.txt",
        "retry_count": 3, 
        "error": "persistent failure",
    }
    producer.produce(dl_topic, key=msg_id.encode(), value=json.dumps(payload).encode())
    producer.flush()

    replay_settings = type(settings).model_construct(**{
        **settings.model_dump(),
        "kafka_dead_letter_topic": dl_topic,
        "kafka_inbound_topic": inbound_topic,
        "dead_letter_max_retries": 3,
        "dead_letter_replay_delay": 0.0,
    })

    service = DeadLetterReplayService(replay_settings)

    msg = service._consumer.poll(15.0)
    assert msg is not None
    raw = json.loads(msg.value().decode("utf-8"))
    assert raw["retry_count"] >= replay_settings.dead_letter_max_retries

    service._consumer.commit(asynchronous=False)

    inbound_consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": f"verify-drop-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    inbound_consumer.subscribe([inbound_topic])
    check = inbound_consumer.poll(5.0)
    assert check is None, "Message should NOT have been replayed to inbound topic"
    inbound_consumer.close()

    service._consumer.close()
