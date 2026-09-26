-- ---------------------------------------------------------------------------
-- BankFlow Analytics - Gold Layer: Fact table (SQLite)
-- ---------------------------------------------------------------------------

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_key      INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id        TEXT NOT NULL UNIQUE,
    account_key            INTEGER NOT NULL,
    date_key                INTEGER NOT NULL,
    transaction_type_key    INTEGER NOT NULL,
    channel_key             INTEGER NOT NULL,
    location_key            INTEGER,
    amount                  REAL NOT NULL,
    currency                TEXT DEFAULT 'INR',
    status                  TEXT NOT NULL,
    merchant_category       TEXT,
    device_id               TEXT,
    anomaly_score           INTEGER DEFAULT 0,
    is_anomaly              INTEGER DEFAULT 0,      -- 0/1 boolean
    anomaly_reasons         TEXT,
    event_timestamp         TEXT NOT NULL,
    ingestion_timestamp     TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (account_key) REFERENCES dim_account(account_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (transaction_type_key) REFERENCES dim_transaction_type(transaction_type_key),
    FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_txn_account ON fact_transactions(account_key);
CREATE INDEX IF NOT EXISTS idx_fact_txn_date ON fact_transactions(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_txn_anomaly ON fact_transactions(is_anomaly);
CREATE INDEX IF NOT EXISTS idx_fact_txn_event_ts ON fact_transactions(event_timestamp);
