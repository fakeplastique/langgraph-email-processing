from typing import TypedDict

from email_processor.models import ClassificationResult, SummaryResult


class EmailAgentState(TypedDict, total=False):
    message_id: str
    sender: str
    recipients: list[str]
    subject: str
    body_blob_path: str
    body: str
    classification: ClassificationResult | None
    summary: SummaryResult | None
    error: str | None
