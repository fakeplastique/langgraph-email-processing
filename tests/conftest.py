import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from config.settings import Settings
from email_processor.agent.nodes import ClassificationOutput, SummaryOutput
from email_processor.models import InboundEmailMessage



def _docker_available():
    try:
        import docker
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


_HAS_DOCKER = _docker_available()


@pytest.fixture(scope="session")
def kafka_container():
    if not _HAS_DOCKER:
        pytest.skip("Docker is not available")
    from testcontainers.kafka import KafkaContainer
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka:
        yield kafka


@pytest.fixture(scope="session")
def postgres_container():
    if not _HAS_DOCKER:
        pytest.skip("Docker is not available")
    from testcontainers.postgres import PostgresContainer
    with PostgresContainer("postgres:16", driver=None) as pg:
        yield pg


@pytest.fixture(scope="session")
def settings(kafka_container, postgres_container):
    bootstrap = kafka_container.get_bootstrap_server()
    pg_url = postgres_container.get_connection_url()
    return Settings(
        kafka_bootstrap_servers=bootstrap,
        pg_dsn=pg_url,
        blob_storage_root="", 
        anthropic_api_key="test-key",
    )


@pytest.fixture
def pg_store(settings):
    from email_processor.pg_store import PgStore

    store = PgStore(settings.pg_dsn)
    store.init_schema(str(project_root / "sql" / "init.sql"))
    yield store
    store.close()


@pytest.fixture
def mock_llm():
    """Mock LLM that returns structured ClassificationOutput and SummaryOutput"""
    llm = MagicMock()

    classification_llm = MagicMock()
    classification_llm.invoke.return_value = ClassificationOutput(
        category="inquiry",
        confidence=0.92,
        labels=["order", "status-check"],
    )
    classification_llm.bind.return_value = classification_llm

    summary_llm = MagicMock()
    summary_llm.invoke.return_value = SummaryOutput(
        summary="Customer inquires about order #12345 shipping status.",
        key_points=["Order #12345", "No shipping confirmation received", "Requests delivery update"],
    )
    summary_llm.bind.return_value = summary_llm

    def with_structured_output(schema):
        if schema is ClassificationOutput:
            return classification_llm
        elif schema is SummaryOutput:
            return summary_llm
        raise ValueError(f"Unexpected schema: {schema}")

    llm.with_structured_output = with_structured_output
    return llm


@pytest.fixture
def sample_inbound_message():
    """Factory fixture for creating InboundEmailMessage instances"""
    def _make(message_id="test-msg-001", **overrides):
        defaults = dict(
            message_id=message_id,
            recipients=["support@example.com"],
            sender="john@example.com",
            subject="Order Status Inquiry",
            body_blob_path="email_001.txt",
        )
        defaults.update(overrides)
        return InboundEmailMessage(**defaults)
    return _make
