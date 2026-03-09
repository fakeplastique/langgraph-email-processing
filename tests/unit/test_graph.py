import pytest

from email_processor.agent.graph import build_graph
from email_processor.blob_store import LocalFileBlobStore


@pytest.fixture
def blob_store(tmp_path):
    store = LocalFileBlobStore(str(tmp_path))
    store.write("test_email.txt", "Hello, this is a test email body about an order inquiry.")
    return store


def test_graph_processes_email(blob_store, mock_llm):
    graph = build_graph(blob_store, mock_llm)

    result = graph.invoke(
        {
            "message_id": "test-123",
            "sender": "john@example.com",
            "recipients": ["support@example.com"],
            "subject": "Order Status Inquiry",
            "body_blob_path": "test_email.txt",
            "body": "",
            "classification": None,
            "summary": None,
            "error": None,
        }
    )

    assert result["body"] == "Hello, this is a test email body about an order inquiry."
    assert result["classification"] is not None
    assert result["classification"].category == "inquiry"
    assert result["classification"].confidence == 0.92
    assert result["summary"] is not None
    assert "order" in result["summary"].summary.lower()
    assert len(result["summary"].key_points) == 3
    assert result["error"] is None


def test_graph_handles_missing_blob(mock_llm, tmp_path):
    blob_store = LocalFileBlobStore(str(tmp_path))
    graph = build_graph(blob_store, mock_llm)

    result = graph.invoke(
        {
            "message_id": "test-456",
            "sender": "john@example.com",
            "recipients": ["support@example.com"],
            "subject": "Test",
            "body_blob_path": "nonexistent.txt",
            "body": "",
            "classification": None,
            "summary": None,
            "error": None,
        }
    )

    assert result["error"] is not None
    assert "Failed to load" in result["error"]


def test_graph_parallel_execution(blob_store, mock_llm):
    """Both classify and summarize should produce results from a single invocation"""
    graph = build_graph(blob_store, mock_llm)

    result = graph.invoke(
        {
            "message_id": "test-789",
            "sender": "john@example.com",
            "recipients": ["support@example.com"],
            "subject": "Parallel Test",
            "body_blob_path": "test_email.txt",
            "body": "",
            "classification": None,
            "summary": None,
            "error": None,
        }
    )

    assert result["classification"] is not None
    assert result["summary"] is not None
    assert result["classification"].message_id == "test-789"
    assert result["summary"].message_id == "test-789"
