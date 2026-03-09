from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "email-processor-group"
    kafka_inbound_topic: str = "emails.inbound"
    kafka_summary_topic: str = "emails.summaries"
    kafka_classification_topic: str = "emails.classifications"
    kafka_dead_letter_topic: str = "emails.dead-letter"
    kafka_auto_offset_reset: str = "earliest"
    kafka_topic_partitions: int = 3

    pg_dsn: str
    blob_storage_root: str

    anthropic_api_key: str = Field(min_length=1)
    llm_model: str = "claude-sonnet-4-20250514"

    llm_retry_max_attempts: int = 4
    llm_retry_initial_wait: float = 1.0
    llm_retry_max_wait: float = 30.0
    llm_retry_jitter: float = 1.0

    pg_retry_max_attempts: int = 3
    pg_retry_wait: float = 0.5

    kafka_producer_retries: int = 5
    kafka_producer_retry_backoff_ms: int = 200
    kafka_flush_retry_max_attempts: int = 3
    kafka_commit_retry_max_attempts: int = 3
    kafka_commit_retry_wait: float = 0.5

    dead_letter_max_retries: int = 3
    dead_letter_replay_delay: float = 30.0

    model_config = {"env_prefix": "APP_", "env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}
