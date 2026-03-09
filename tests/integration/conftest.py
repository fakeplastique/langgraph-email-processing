import uuid

import pytest
from confluent_kafka.admin import AdminClient, NewTopic


@pytest.fixture
def unique_topic_prefix():
    """Generate a unique prefix for Kafka topics to avoid cross-test pollution"""
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def create_topics(settings):
    """Factory fixture to create Kafka topics for integration tests"""
    created = []

    def _create(*topic_names):
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topic_names]
        futures = admin.create_topics(topics)
        for topic, future in futures.items():
            try:
                future.result()
            except Exception:
                pass
        created.extend(topic_names)
        return topic_names

    yield _create


@pytest.fixture
def unique_group_id():
    """Unique consumer group ID to prevent interference between tests"""
    return f"test-group-{uuid.uuid4().hex[:8]}"
