import logging
from datetime import UTC, datetime

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.language_models import BaseChatModel
from pydantic import BaseModel, Field

from email_processor.blob_store import BlobStore
from email_processor.models import ClassificationResult, SummaryResult

logger = logging.getLogger(__name__)



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


def classify(state: dict, *, llm: BaseChatModel) -> dict:
    if state.get("error"):
        return {}

    structured_llm = llm.with_structured_output(ClassificationOutput)
    result = structured_llm.invoke(
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


def summarize(state: dict, *, llm: BaseChatModel) -> dict:
    if state.get("error"):
        return {}

    structured_llm = llm.with_structured_output(SummaryOutput)
    result = structured_llm.invoke(
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
