FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev gcc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/
COPY config/ config/
COPY sql/ sql/

RUN pip install --no-cache-dir .

RUN useradd -m appuser && mkdir -p /data/blobs && chown -R appuser:appuser /data/blobs
USER appuser

ENV PYTHONPATH=/app/src:/app
ENTRYPOINT ["python", "-m", "email_processor.main"]
