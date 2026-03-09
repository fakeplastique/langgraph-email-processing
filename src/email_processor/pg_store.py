import json
import logging
from pathlib import Path

import psycopg2
import psycopg2.extras
import psycopg2.pool

from email_processor.models import EmailProcessingRecord

logger = logging.getLogger(__name__)


class PgStore:
    def __init__(self, dsn: str):
        logger.info("Initializing PostgreSQL connection pool")
        self._pool = psycopg2.pool.ThreadedConnectionPool(1, 10, dsn)
        logger.info("PostgreSQL connection pool ready")

    def init_schema(self, sql_path: str) -> None:
        logger.info("Initializing DB schema from %s", sql_path)
        sql = Path(sql_path).read_text(encoding="utf-8")
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        finally:
            self._pool.putconn(conn)

    def upsert_result(self, record: EmailProcessingRecord) -> None:
        logger.debug("Upserting record for message_id=%s status=%s", record.message_id, record.status)
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO email_processing
                        (message_id, sender, recipients, subject,
                         summary, key_points, category, confidence, labels,
                         status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (message_id) DO UPDATE SET
                        summary = EXCLUDED.summary,
                        key_points = EXCLUDED.key_points,
                        category = EXCLUDED.category,
                        confidence = EXCLUDED.confidence,
                        labels = EXCLUDED.labels,
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message,
                        updated_at = NOW()
                    """,
                    (
                        record.message_id,
                        record.sender,
                        json.dumps(record.recipients),
                        record.subject,
                        record.summary,
                        json.dumps(record.key_points),
                        record.category,
                        record.confidence,
                        json.dumps(record.labels),
                        record.status,
                        record.error_message,
                    ),
                )
            conn.commit()
        finally:
            self._pool.putconn(conn)

    def get_by_message_id(self, message_id: str) -> EmailProcessingRecord | None:
        conn = self._pool.getconn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM email_processing WHERE message_id = %s",
                    (message_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return EmailProcessingRecord(
                    id=row["id"],
                    message_id=row["message_id"],
                    sender=row["sender"],
                    recipients=row["recipients"],
                    subject=row["subject"],
                    summary=row["summary"],
                    key_points=row["key_points"] or [],
                    category=row["category"],
                    confidence=row["confidence"],
                    labels=row["labels"] or [],
                    status=row["status"],
                    error_message=row["error_message"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        logger.info("Closing PostgreSQL connection pool")
        self._pool.closeall()
