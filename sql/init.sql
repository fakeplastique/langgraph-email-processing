CREATE TABLE IF NOT EXISTS email_processing (
    id              BIGSERIAL PRIMARY KEY,
    message_id      VARCHAR(255) UNIQUE NOT NULL,
    sender          VARCHAR(512) NOT NULL,
    recipients      JSONB NOT NULL,
    subject         TEXT NOT NULL,
    summary         TEXT,
    key_points      JSONB DEFAULT '[]',
    category        VARCHAR(128),
    confidence      DOUBLE PRECISION,
    labels          JSONB DEFAULT '[]',
    status          VARCHAR(32) NOT NULL DEFAULT 'pending',
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_email_processing_message_id ON email_processing(message_id);
CREATE INDEX IF NOT EXISTS idx_email_processing_status ON email_processing(status);
