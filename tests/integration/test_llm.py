import os

import pytest
from langchain_anthropic import ChatAnthropic

from email_processor.agent.graph import build_graph
from email_processor.blob_store import LocalFileBlobStore

pytestmark = pytest.mark.skipif(
    not os.environ.get("APP_ANTHROPIC_API_KEY"),
    reason="APP_ANTHROPIC_API_KEY not set",
)


@pytest.fixture
def llm():
    return ChatAnthropic(
        model="claude-sonnet-4-20250514",
        api_key=os.environ["APP_ANTHROPIC_API_KEY"],
    )


@pytest.fixture
def blob_store(tmp_path):
    store = LocalFileBlobStore(str(tmp_path))
    store.write(
        "order_inquiry.txt",
        "Dear Support,\n\n"
        "I placed order #98765 three days ago but haven't received a shipping confirmation.\n"
        "Could you please check the status?\n\n"
        "Best regards,\nJane Smith",
    )
    return store


def test_graph_with_real_llm(blob_store, llm):
    """Full graph with real Claude: classify + summarize a sample email"""
    graph = build_graph(blob_store, llm)

    result = graph.invoke({
        "message_id": "llm-test-001",
        "sender": "jane@example.com",
        "recipients": ["support@shop.com"],
        "subject": "Order #98765 — shipping status?",
        "body_blob_path": "order_inquiry.txt",
        "body": "",
        "classification": None,
        "summary": None,
        "error": None,
    })

    assert result["error"] is None

    cls = result["classification"]
    assert cls is not None
    assert cls.category in ("inquiry", "order")
    assert 0.0 <= cls.confidence <= 1.0
    assert len(cls.labels) > 0

    summ = result["summary"]
    assert summ is not None
    assert len(summ.summary) > 10
    assert len(summ.key_points) > 0
