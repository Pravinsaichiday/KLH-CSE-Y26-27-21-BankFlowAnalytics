-- ---------------------------------------------------------------------------
-- BankFlow Analytics - Gold Layer: KPI aggregate tables (SQLite)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS daily_transaction_metrics (
    metric_key           INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key              INTEGER NOT NULL,
    total_transactions    INTEGER NOT NULL DEFAULT 0,
    total_volume          REAL NOT NULL DEFAULT 0,
    success_count         INTEGER NOT NULL DEFAULT 0,
    failed_count          INTEGER NOT NULL DEFAULT 0,
    success_rate          REAL NOT NULL DEFAULT 0,
    anomaly_count         INTEGER NOT NULL DEFAULT 0,
    anomaly_rate          REAL NOT NULL DEFAULT 0,
    avg_transaction_value REAL NOT NULL DEFAULT 0,
    computed_at            TEXT DEFAULT (datetime('now')),
    UNIQUE(date_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

CREATE TABLE IF NOT EXISTS hourly_transaction_metrics (
    metric_key           INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key              INTEGER NOT NULL,
    hour_of_day           INTEGER NOT NULL,
    total_transactions    INTEGER NOT NULL DEFAULT 0,
    total_volume          REAL NOT NULL DEFAULT 0,
    anomaly_count         INTEGER NOT NULL DEFAULT 0,
    failed_count          INTEGER NOT NULL DEFAULT 0,
    computed_at            TEXT DEFAULT (datetime('now')),
    UNIQUE(date_key, hour_of_day),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

CREATE TABLE IF NOT EXISTS account_risk_summary (
    summary_key           INTEGER PRIMARY KEY AUTOINCREMENT,
    account_key            INTEGER NOT NULL,
    total_transactions     INTEGER NOT NULL DEFAULT 0,
    total_volume           REAL NOT NULL DEFAULT 0,
    anomaly_count          INTEGER NOT NULL DEFAULT 0,
    max_anomaly_score      INTEGER NOT NULL DEFAULT 0,
    avg_anomaly_score      REAL NOT NULL DEFAULT 0,
    last_transaction_ts    TEXT,
    risk_band              TEXT DEFAULT 'NORMAL',
    computed_at             TEXT DEFAULT (datetime('now')),
    UNIQUE(account_key),
    FOREIGN KEY (account_key) REFERENCES dim_account(account_key)
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_transaction_metrics(date_key);
CREATE INDEX IF NOT EXISTS idx_hourly_metrics_date ON hourly_transaction_metrics(date_key, hour_of_day);
CREATE INDEX IF NOT EXISTS idx_account_risk_score ON account_risk_summary(max_anomaly_score);
