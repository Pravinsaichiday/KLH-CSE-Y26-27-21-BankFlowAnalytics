"""
Integration tests for the Silver -> Gold lineage: schema initialization,
dimension/fact loads, and KPI recomputation, run against a throwaway
temporary SQLite database (no external services required).
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.gold.sqlite_loader import initialize_schema
from src.gold.dimensions import load_all_dimensions
from src.gold.facts import load_fact_transactions
from src.gold.metrics import recompute_all_metrics


@pytest.fixture
def temp_engine(tmp_path):
    db_path = tmp_path / "bankflow_gold_test.db"
    engine = create_engine(f"sqlite:///{db_path}", future=True)
    initialize_schema(engine)
    return engine


@pytest.fixture
def sample_silver_df():
    return pd.DataFrame(
        [
            {
                "transaction_id": "TXN-1",
                "account_id": "ACC1",
                "transaction_type": "TRANSFER",
                "amount": 100.0,
                "currency": "INR",
                "event_timestamp": "2026-01-01T10:00:00.000Z",
                "channel": "MOBILE",
                "status": "SUCCESS",
                "merchant_category": "GROCERY",
                "city": "Mumbai",
                "device_id": "DEV-1",
                "anomaly_score": 0,
                "is_anomaly": False,
                "anomaly_reasons": "",
            },
            {
                "transaction_id": "TXN-2",
                "account_id": "ACC2",
                "transaction_type": "UPI_PAYMENT",
                "amount": 5000.0,
                "currency": "INR",
                "event_timestamp": "2026-01-01T02:00:00.000Z",
                "channel": "UPI",
                "status": "FAILED",
                "merchant_category": "ECOMMERCE",
                "city": "Delhi",
                "device_id": "DEV-2",
                "anomaly_score": 5,
                "is_anomaly": True,
                "anomaly_reasons": "HIGH_AMOUNT,UNUSUAL_HOUR",
            },
        ]
    )


def test_schema_initialization_creates_all_tables(temp_engine):
    with temp_engine.connect() as conn:
        tables = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table'")
        ).fetchall()
    table_names = {row[0] for row in tables}

    expected = {
        "dim_account", "dim_date", "dim_transaction_type", "dim_channel",
        "dim_location", "fact_transactions", "daily_transaction_metrics",
        "hourly_transaction_metrics", "account_risk_summary",
    }
    assert expected.issubset(table_names)


def test_schema_initialization_is_idempotent(temp_engine):
    # Running it again must not raise.
    initialize_schema(temp_engine)


def test_dimension_load_populates_expected_rows(temp_engine, sample_silver_df):
    ts = pd.to_datetime(sample_silver_df["event_timestamp"])
    load_all_dimensions(
        temp_engine, sample_silver_df, ts.min().to_pydatetime(), ts.max().to_pydatetime()
    )

    with temp_engine.connect() as conn:
        account_count = conn.execute(text("SELECT COUNT(*) FROM dim_account")).scalar()
        date_count = conn.execute(text("SELECT COUNT(*) FROM dim_date")).scalar()
        txn_type_count = conn.execute(text("SELECT COUNT(*) FROM dim_transaction_type")).scalar()
        location_count = conn.execute(text("SELECT COUNT(*) FROM dim_location")).scalar()

    assert account_count == 2
    assert date_count == 1  # both timestamps fall on 2026-01-01
    assert txn_type_count == 2
    assert location_count == 2


def test_fact_load_inserts_rows_and_is_idempotent(temp_engine, sample_silver_df):
    ts = pd.to_datetime(sample_silver_df["event_timestamp"])
    load_all_dimensions(
        temp_engine, sample_silver_df, ts.min().to_pydatetime(), ts.max().to_pydatetime()
    )
    inserted_first = load_fact_transactions(temp_engine, sample_silver_df)
    inserted_second = load_fact_transactions(temp_engine, sample_silver_df)

    with temp_engine.connect() as conn:
        fact_count = conn.execute(text("SELECT COUNT(*) FROM fact_transactions")).scalar()

    assert inserted_first == 2
    assert inserted_second == 0  # ON CONFLICT DO NOTHING -> no duplicate rows
    assert fact_count == 2


def test_kpi_recompute_produces_daily_and_risk_summaries(temp_engine, sample_silver_df):
    ts = pd.to_datetime(sample_silver_df["event_timestamp"])
    load_all_dimensions(
        temp_engine, sample_silver_df, ts.min().to_pydatetime(), ts.max().to_pydatetime()
    )
    load_fact_transactions(temp_engine, sample_silver_df)
    result = recompute_all_metrics(temp_engine)

    assert result["daily"] >= 1
    assert result["account_risk"] == 2

    with temp_engine.connect() as conn:
        daily_row = conn.execute(
            text("SELECT total_transactions, anomaly_count FROM daily_transaction_metrics")
        ).fetchone()
    assert daily_row.total_transactions == 2
    assert daily_row.anomaly_count == 1


def test_end_to_end_silver_to_gold_lineage(temp_engine, sample_silver_df):
    """Full Silver -> Gold path in one shot, verifying referential integrity."""
    ts = pd.to_datetime(sample_silver_df["event_timestamp"])
    load_all_dimensions(
        temp_engine, sample_silver_df, ts.min().to_pydatetime(), ts.max().to_pydatetime()
    )
    load_fact_transactions(temp_engine, sample_silver_df)
    recompute_all_metrics(temp_engine)

    with temp_engine.connect() as conn:
        orphaned = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM fact_transactions f
                LEFT JOIN dim_account a ON f.account_key = a.account_key
                WHERE a.account_key IS NULL
                """
            )
        ).scalar()
        inconsistent = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM fact_transactions
                WHERE (is_anomaly = 1 AND anomaly_score < 5)
                   OR (is_anomaly = 0 AND anomaly_score >= 5)
                """
            )
        ).scalar()

    assert orphaned == 0
    assert inconsistent == 0
