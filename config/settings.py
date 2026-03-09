from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "email-processor-group"
    kafka_inbound_topic: str = "emails.inbound"
    kafka_summary_topic: str = "emails.summaries"
    kafka_classification_topic: str = "emails.classifications"
    kafka_auto_offset_reset: str = "earliest"
    kafka_topic_partitions: int = 3

    pg_dsn: str
    blob_storage_root: str

    anthropic_api_key: str = Field(min_length=1)
    llm_model: str = "claude-sonnet-4-20250514"

    model_config = {"env_prefix": "APP_", "env_file": ".env", "env_file_encoding": "utf-8"}
