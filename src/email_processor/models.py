from datetime import datetime

from pydantic import BaseModel, Field


class InboundEmailMessage(BaseModel):
    """Kafka inbound message: email metadata + blob path."""

    message_id: str
    recipients: list[str]
    sender: str
    subject: str
    body_blob_path: str
    received_at: datetime = Field(default_factory=datetime.utcnow)


class SummaryResult(BaseModel):
    """Produced to the summaries Kafka topic"""

    message_id: str
    summary: str
    key_points: list[str]
    processed_at: datetime = Field(default_factory=datetime.utcnow)


class ClassificationResult(BaseModel):
    """Produced to the classifications Kafka topic"""

    message_id: str
    category: str
    confidence: float
    labels: list[str]
    processed_at: datetime = Field(default_factory=datetime.utcnow)


class EmailProcessingRecord(BaseModel):
    """Mirrors the email_processing DB table"""

    id: int | None = None
    message_id: str
    sender: str
    recipients: list[str]
    subject: str
    summary: str | None = None
    key_points: list[str] = []
    category: str | None = None
    confidence: float | None = None
    labels: list[str] = []
    status: str = "pending"
    error_message: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
