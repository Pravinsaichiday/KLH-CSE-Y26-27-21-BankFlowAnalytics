"""
Unit tests for src/streaming/transformations.py — dedup, time features, and
rolling window aggregation logic.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F

from src.streaming.transformations import (
    deduplicate,
    add_time_features,
    compute_rolling_window_aggregates,
    join_transaction_with_window_stats,
)


def _txn_row(**overrides):
    base = dict(
        transaction_id="TXN-1",
        account_id="ACC1",
        amount=100.0,
        status="SUCCESS",
        device_id="DEV-1",
        city="Mumbai",
        event_ts=datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return Row(**base)


def test_deduplicate_removes_repeated_transaction_ids(spark):
    rows = [_txn_row(transaction_id="TXN-1"), _txn_row(transaction_id="TXN-1")]
    df = spark.createDataFrame(rows)
    deduped = deduplicate(df)
    assert deduped.count() == 1


def test_deduplicate_keeps_distinct_transaction_ids(spark):
    rows = [_txn_row(transaction_id="TXN-1"), _txn_row(transaction_id="TXN-2")]
    df = spark.createDataFrame(rows)
    deduped = deduplicate(df)
    assert deduped.count() == 2


def test_add_time_features_extracts_hour_and_date(spark):
    df = spark.createDataFrame(
        [_txn_row(event_ts=datetime(2026, 3, 15, 23, 30, 0, tzinfo=timezone.utc))]
    )
    result = add_time_features(df).collect()[0]
    assert result["event_hour"] == 23
    assert str(result["event_date"]) == "2026-03-15"
    assert result["event_minute"] == 30


def test_rolling_window_counts_transactions_per_account(spark):
    base_time = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    rows = [
        _txn_row(transaction_id=f"TXN-{i}", account_id="ACC1", event_ts=base_time)
        for i in range(5)
    ]
    df = spark.createDataFrame(rows)
    window_df = compute_rolling_window_aggregates(df)
    totals = window_df.agg(F.max("window_txn_count").alias("max_count")).collect()[0]
    assert totals["max_count"] >= 5


def test_join_transaction_with_window_stats_preserves_row_count(spark):
    base_time = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    rows = [_txn_row(transaction_id="TXN-1", event_ts=base_time)]
    df = spark.createDataFrame(rows)
    window_df = compute_rolling_window_aggregates(df)
    joined = join_transaction_with_window_stats(df, window_df)
    assert joined.count() == 1
    assert "window_txn_count" in joined.columns
