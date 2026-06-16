CREATE TABLE IF NOT EXISTS ledger_events (
    ledger_id SERIAL PRIMARY KEY,
    event_id TEXT UNIQUE NOT NULL,
    event_type TEXT NOT NULL,
    source_network TEXT NOT NULL,
    payload JSONB NOT NULL,
    previous_hash TEXT,
    event_hash TEXT NOT NULL,
    validation_status TEXT NOT NULL,
    rejection_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);