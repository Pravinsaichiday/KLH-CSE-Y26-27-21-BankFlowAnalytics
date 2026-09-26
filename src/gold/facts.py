"""
Gold layer: fact_transactions loader.

Resolves surrogate keys for each dimension and inserts/upserts rows into
fact_transactions, keyed by the natural `transaction_id`.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.logging_config import get_logger
from src.common.utils import date_key_from_iso

logger = get_logger(__name__)


def _lookup_map(engine: Engine, table: str, key_col: str, value_col: str) -> dict:
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT {key_col}, {value_col} FROM {table}"), conn)
    return dict(zip(df[value_col], df[key_col]))


def load_fact_transactions(engine: Engine, silver_pdf: pd.DataFrame) -> int:
    """
    Load a batch of Silver rows (as a pandas DataFrame) into fact_transactions.
    Returns the number of rows inserted (duplicates on transaction_id are
    skipped via ON CONFLICT DO NOTHING, since Silver is append-only and a
    given transaction_id should only ever map to one fact row).
    """
    if silver_pdf.empty:
        return 0

    account_map = _lookup_map(engine, "dim_account", "account_key", "account_id")
    txn_type_map = _lookup_map(
        engine, "dim_transaction_type", "transaction_type_key", "transaction_type"
    )
    channel_map = _lookup_map(engine, "dim_channel", "channel_key", "channel")

    with engine.connect() as conn:
        location_df = pd.read_sql(text("SELECT location_key, city FROM dim_location"), conn)
    location_map = dict(zip(location_df["city"], location_df["location_key"]))

    inserted = 0
    with engine.begin() as conn:
        for _, row in silver_pdf.iterrows():
            date_key = date_key_from_iso(row.get("event_timestamp"))
            account_key = account_map.get(row.get("account_id"))
            txn_type_key = txn_type_map.get(row.get("transaction_type"))
            channel_key = channel_map.get(row.get("channel"))
            location_key = location_map.get(row.get("city"))

            if date_key is None or account_key is None or txn_type_key is None:
                logger.warning(
                    "Skipping fact row for transaction_id=%s due to unresolved dimension key "
                    "(date_key=%s, account_key=%s, txn_type_key=%s)",
                    row.get("transaction_id"),
                    date_key,
                    account_key,
                    txn_type_key,
                )
                continue

            result = conn.execute(
                text(
                    """
                    INSERT INTO fact_transactions (
                        transaction_id, account_key, date_key, transaction_type_key,
                        channel_key, location_key, amount, currency, status,
                        merchant_category, device_id, anomaly_score, is_anomaly,
                        anomaly_reasons, event_timestamp
                    ) VALUES (
                        :transaction_id, :account_key, :date_key, :transaction_type_key,
                        :channel_key, :location_key, :amount, :currency, :status,
                        :merchant_category, :device_id, :anomaly_score, :is_anomaly,
                        :anomaly_reasons, :event_timestamp
                    )
                    ON CONFLICT(transaction_id) DO NOTHING
                    """
                ),
                {
                    "transaction_id": row.get("transaction_id"),
                    "account_key": account_key,
                    "date_key": date_key,
                    "transaction_type_key": txn_type_key,
                    "channel_key": channel_key,
                    "location_key": location_key,
                    "amount": float(row.get("amount", 0.0)),
                    "currency": row.get("currency", "INR"),
                    "status": row.get("status"),
                    "merchant_category": row.get("merchant_category"),
                    "device_id": row.get("device_id"),
                    "anomaly_score": int(row.get("anomaly_score", 0) or 0),
                    "is_anomaly": 1 if bool(row.get("is_anomaly", False)) else 0,
                    "anomaly_reasons": row.get("anomaly_reasons") or "",
                    "event_timestamp": row.get("event_timestamp"),
                },
            )
            inserted += result.rowcount if result.rowcount is not None else 0

    logger.info("fact_transactions: inserted %d new rows (of %d candidates)", inserted, len(silver_pdf))
    return inserted
