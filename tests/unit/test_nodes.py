from unittest.mock import MagicMock

import pytest

from email_processor.agent.nodes import (
    ClassificationOutput,
    SummaryOutput,
    classify,
    load_body,
    make_llm_retry,
    summarize,
)
from email_processor.blob_store import LocalFileBlobStore
from email_processor.models import ClassificationResult, SummaryResult


def _base_state(**overrides):
    state = {
        "message_id": "node-test-001",
        "sender": "alice@example.com",
        "recipients": ["bob@example.com"],
        "subject": "Test Subject",
        "body_blob_path": "email.txt",
        "body": "This is the email body.",
        "classification": None,
        "summary": None,
        "error": None,
    }
    state.update(overrides)
    return state


class TestLoadBody:
    def test_success(self, tmp_path):
        store = LocalFileBlobStore(str(tmp_path))
        store.write("email.txt", "Hello from email")
        result = load_body(_base_state(), blob_store=store)
        assert result["body"] == "Hello from email"
        assert "error" not in result

    def test_missing_file(self, tmp_path):
        store = LocalFileBlobStore(str(tmp_path))
        result = load_body(_base_state(body_blob_path="missing.txt"), blob_store=store)
        assert result["body"] == ""
        assert "Failed to load" in result["error"]


class TestClassify:
    def test_success(self):
        mock_invoke = MagicMock(return_value=ClassificationOutput(
            category="complaint", confidence=0.85, labels=["billing"]
        ))
        result = classify(_base_state(), llm_invoke=mock_invoke)
        assert isinstance(result["classification"], ClassificationResult)
        assert result["classification"].category == "complaint"
        assert result["classification"].confidence == 0.85
        assert result["classification"].message_id == "node-test-001"
        mock_invoke.assert_called_once()

    def test_skips_on_error(self):
        mock_invoke = MagicMock()
        result = classify(_base_state(error="some error"), llm_invoke=mock_invoke)
        assert result == {}
        mock_invoke.assert_not_called()


class TestSummarize:
    def test_success(self):
        mock_invoke = MagicMock(return_value=SummaryOutput(
            summary="A concise summary.", key_points=["point1"]
        ))
        result = summarize(_base_state(), llm_invoke=mock_invoke)
        assert isinstance(result["summary"], SummaryResult)
        assert result["summary"].summary == "A concise summary."
        assert result["summary"].message_id == "node-test-001"
        mock_invoke.assert_called_once()

    def test_skips_on_error(self):
        mock_invoke = MagicMock()
        result = summarize(_base_state(error="blob failed"), llm_invoke=mock_invoke)
        assert result == {}
        mock_invoke.assert_not_called()


class TestMakeLlmRetry:
    def test_builds_callable_decorator(self):
        decorator = make_llm_retry(max_attempts=2, initial=0.1, max_wait=1.0, jitter=0.0)
        assert callable(decorator)

    def test_decorated_function_succeeds(self):
        decorator = make_llm_retry(max_attempts=2, initial=0.01, max_wait=0.1, jitter=0.0)

        call_count = 0

        @decorator
        def flaky():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert flaky() == "ok"
        assert call_count == 1
