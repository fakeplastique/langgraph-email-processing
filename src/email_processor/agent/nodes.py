import logging
from datetime import UTC, datetime
from typing import Callable

from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
    before_sleep_log,
)

from email_processor.blob_store import BlobStore
from email_processor.models import ClassificationResult, SummaryResult
from anthropic import RateLimitError, InternalServerError, APIConnectionError, APITimeoutError

logger = logging.getLogger(__name__)


_LLM_RETRYABLE_ERRORS: tuple[type[Exception], ...] = (RateLimitError, InternalServerError, APIConnectionError, APITimeoutError)



def make_llm_retry(
    max_attempts: int = 4,
    initial: float = 1.0,
    max_wait: float = 30.0,
    jitter: float = 1.0,
):
    """Build a tenacity retry decorator for LLM calls. Call once at startup."""
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential_jitter(initial=initial, max=max_wait, jitter=jitter),
        retry=retry_if_exception_type(_LLM_RETRYABLE_ERRORS) if _LLM_RETRYABLE_ERRORS else retry_if_exception_type(()),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


class ClassificationOutput(BaseModel):
    category: str = Field()
    confidence: float = Field(ge=0.0, le=1.0)
    labels: list[str] = Field(description="Multi-label tags describing the email")


class SummaryOutput(BaseModel):
    summary: str = Field(description="Concise summary of the email")
    key_points: list[str] = Field(description="Key points extracted from the email")


def load_body(state: dict, *, blob_store: BlobStore) -> dict:
    try:
        body = blob_store.read(state["body_blob_path"])
        return {"body": body}
    except Exception as e:
        logger.error("Failed to load body blob: %s", e)
        return {"body": "", "error": f"Failed to load email body: {e}"}


def classify(state: dict, *, llm_invoke: Callable) -> dict:
    if state.get("error"):
        return {}

    result = llm_invoke(
        [
            SystemMessage(
                content=(
                    "You are an email classifier. Classify the email into one of these categories: "
                    "inquiry, complaint, spam, order, internal, newsletter, other. "
                    "Also assign a confidence score (0-1) and relevant labels."
                )
            ),
            HumanMessage(
                content=(
                    f"From: {state['sender']}\n"
                    f"To: {', '.join(state['recipients'])}\n"
                    f"Subject: {state['subject']}\n\n"
                    f"{state['body']}"
                )
            ),
        ]
    )
    return {
        "classification": ClassificationResult(
            message_id=state["message_id"],
            category=result.category,
            confidence=result.confidence,
            labels=result.labels,
            processed_at=datetime.now(UTC),
        )
    }


def summarize(state: dict, *, llm_invoke: Callable) -> dict:
    if state.get("error"):
        return {}

    result = llm_invoke(
        [
            SystemMessage(
                content=(
                    "You are an email summarizer. Provide a concise summary and extract key points. "
                    "Keep the summary to 2-3 sentences maximum."
                )
            ),
            HumanMessage(
                content=(
                    f"From: {state['sender']}\n"
                    f"To: {', '.join(state['recipients'])}\n"
                    f"Subject: {state['subject']}\n\n"
                    f"{state['body']}"
                )
            ),
        ]
    )
    return {
        "summary": SummaryResult(
            message_id=state["message_id"],
            summary=result.summary,
            key_points=result.key_points,
            processed_at=datetime.now(UTC),
        )
    }
