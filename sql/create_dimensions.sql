-- ---------------------------------------------------------------------------
-- BankFlow Analytics - Gold Layer: Dimension tables (SQLite)
-- Star schema dimensions, all surrogate-keyed.
-- ---------------------------------------------------------------------------

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_account (
    account_key         INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id          TEXT NOT NULL UNIQUE,
    account_type        TEXT,
    customer_segment    TEXT,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dim_account_account_id ON dim_account(account_id);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date           TEXT NOT NULL UNIQUE,  -- YYYY-MM-DD
    day                 INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    month_name          TEXT NOT NULL,
    quarter             INTEGER NOT NULL,
    year                INTEGER NOT NULL,
    day_of_week         INTEGER NOT NULL,      -- 0=Monday ... 6=Sunday
    day_name            TEXT NOT NULL,
    is_weekend          INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dim_transaction_type (
    transaction_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_type      TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_key         INTEGER PRIMARY KEY AUTOINCREMENT,
    channel             TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_key        INTEGER PRIMARY KEY AUTOINCREMENT,
    city                TEXT NOT NULL,
    region               TEXT,
    UNIQUE(city, region)
);
