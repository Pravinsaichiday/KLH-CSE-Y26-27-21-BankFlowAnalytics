"""
Gold layer: dimension table loaders (SQLAlchemy + SQLite).

Reads distinct dimension values out of the Silver Delta table (via a Spark
batch read passed in by the caller, or a plain pandas DataFrame) and
upserts them into the SQLite star-schema dimension tables, assigning
surrogate keys.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.logging_config import get_logger

logger = get_logger(__name__)

_DAY_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
]
_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def load_dim_account(engine: Engine, silver_pdf: pd.DataFrame) -> None:
    """
    Upsert dim_account from distinct (account_id) values seen in the Silver
    batch. account_type / customer_segment aren't present on the transaction
    stream itself (they're generator-side attributes), so new accounts are
    inserted with sensible defaults that can be enriched later by a
    reference-data load if one becomes available.
    """
    if silver_pdf.empty:
        return

    account_ids = silver_pdf["account_id"].dropna().unique().tolist()
    if not account_ids:
        return

    with engine.begin() as conn:
        existing = pd.read_sql(
            text("SELECT account_id FROM dim_account"), conn
        )["account_id"].tolist()
        new_ids = [a for a in account_ids if a not in existing]

        for account_id in new_ids:
            conn.execute(
                text(
                    """
                    INSERT INTO dim_account (account_id, account_type, customer_segment)
                    VALUES (:account_id, :account_type, :customer_segment)
                    ON CONFLICT(account_id) DO NOTHING
                    """
                ),
                {
                    "account_id": account_id,
                    "account_type": "UNKNOWN",
                    "customer_segment": "UNKNOWN",
                },
            )
    logger.info("dim_account: upserted %d new accounts", len(new_ids))


def load_dim_date(engine: Engine, start_date: datetime, end_date: datetime) -> None:
    """
    Populate dim_date for every calendar day in [start_date, end_date]
    (inclusive), idempotently.
    """
    rows = []
    current = start_date
    while current <= end_date:
        date_key = int(current.strftime("%Y%m%d"))
        rows.append(
            {
                "date_key": date_key,
                "full_date": current.strftime("%Y-%m-%d"),
                "day": current.day,
                "month": current.month,
                "month_name": _MONTH_NAMES[current.month - 1],
                "quarter": (current.month - 1) // 3 + 1,
                "year": current.year,
                "day_of_week": current.weekday(),
                "day_name": _DAY_NAMES[current.weekday()],
                "is_weekend": 1 if current.weekday() >= 5 else 0,
            }
        )
        current += timedelta(days=1)

    if not rows:
        return

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO dim_date
                        (date_key, full_date, day, month, month_name, quarter, year,
                         day_of_week, day_name, is_weekend)
                    VALUES
                        (:date_key, :full_date, :day, :month, :month_name, :quarter, :year,
                         :day_of_week, :day_name, :is_weekend)
                    ON CONFLICT(date_key) DO NOTHING
                    """
                ),
                row,
            )
    logger.info("dim_date: upserted %d date rows", len(rows))


def load_dim_transaction_type(engine: Engine, silver_pdf: pd.DataFrame) -> None:
    _upsert_simple_dim(
        engine,
        silver_pdf,
        source_col="transaction_type",
        table="dim_transaction_type",
        target_col="transaction_type",
    )


def load_dim_channel(engine: Engine, silver_pdf: pd.DataFrame) -> None:
    _upsert_simple_dim(
        engine, silver_pdf, source_col="channel", table="dim_channel", target_col="channel"
    )


def load_dim_location(engine: Engine, silver_pdf: pd.DataFrame) -> None:
    if silver_pdf.empty or "city" not in silver_pdf.columns:
        return

    cities = silver_pdf["city"].dropna().unique().tolist()
    if not cities:
        return

    with engine.begin() as conn:
        for city in cities:
            conn.execute(
                text(
                    """
                    INSERT INTO dim_location (city, region)
                    VALUES (:city, :region)
                    ON CONFLICT(city, region) DO NOTHING
                    """
                ),
                {"city": city, "region": None},
            )
    logger.info("dim_location: upserted %d cities", len(cities))


def _upsert_simple_dim(
    engine: Engine,
    silver_pdf: pd.DataFrame,
    source_col: str,
    table: str,
    target_col: str,
) -> None:
    if silver_pdf.empty or source_col not in silver_pdf.columns:
        return

    values: Iterable[str] = silver_pdf[source_col].dropna().unique().tolist()
    if not values:
        return

    with engine.begin() as conn:
        for value in values:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {table} ({target_col})
                    VALUES (:value)
                    ON CONFLICT({target_col}) DO NOTHING
                    """
                ),
                {"value": value},
            )
    logger.info("%s: upserted %d distinct values", table, len(list(values)))


def load_all_dimensions(
    engine: Engine, silver_pdf: pd.DataFrame, start_date: datetime, end_date: datetime
) -> None:
    """Convenience helper: load every dimension table in one call."""
    load_dim_date(engine, start_date, end_date)
    load_dim_account(engine, silver_pdf)
    load_dim_transaction_type(engine, silver_pdf)
    load_dim_channel(engine, silver_pdf)
    load_dim_location(engine, silver_pdf)
