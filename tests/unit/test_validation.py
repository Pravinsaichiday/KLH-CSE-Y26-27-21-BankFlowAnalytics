"""
Unit tests for src/streaming/validation.py — verifies the valid/invalid
record split logic against each quarantine trigger condition.
"""

from __future__ import annotations

from src.streaming.validation import split_valid_invalid
from src.streaming.schema import RAW_TRANSACTION_SCHEMA


def _row(**overrides):
    base = {
        "transaction_id": "TXN-1",
        "account_id": "ACC0000000001",
        "transaction_type": "TRANSFER",
        "amount": "100.50",
        "currency": "INR",
        "timestamp": "2026-01-01T10:00:00.000Z",
        "channel": "MOBILE",
        "status": "SUCCESS",
        "merchant_category": "GROCERY",
        "city": "Mumbai",
        "device_id": "DEV-1",
        "source": "synthetic-generator",
        "_injected_violation": None,
    }
    base.update(overrides)
    return base


def test_valid_record_passes_through(spark):
    df = spark.createDataFrame([_row()], schema=RAW_TRANSACTION_SCHEMA)
    valid_df, invalid_df = split_valid_invalid(df)
    assert valid_df.count() == 1
    assert invalid_df.count() == 0

    row = valid_df.collect()[0]
    assert row["transaction_id"] == "TXN-1"
    assert row["amount"] == 100.50


def test_negative_amount_is_quarantined(spark):
    df = spark.createDataFrame([_row(amount="-50.00")], schema=RAW_TRANSACTION_SCHEMA)
    valid_df, invalid_df = split_valid_invalid(df)
    assert valid_df.count() == 0
    assert invalid_df.count() == 1
    assert "INVALID_AMOUNT" in invalid_df.collect()[0]["failure_reason"]


def test_zero_amount_is_quarantined(spark):
    df = spark.createDataFrame([_row(amount="0")], schema=RAW_TRANSACTION_SCHEMA)
    _, invalid_df = split_valid_invalid(df)
    assert invalid_df.count() == 1


def test_null_transaction_id_is_quarantined(spark):
    df = spark.createDataFrame([_row(transaction_id=None)], schema=RAW_TRANSACTION_SCHEMA)
    _, invalid_df = split_valid_invalid(df)
    assert invalid_df.count() == 1
    assert "MISSING_REQUIRED_FIELD" in invalid_df.collect()[0]["failure_reason"]


def test_invalid_timestamp_is_quarantined(spark):
    df = spark.createDataFrame(
        [_row(timestamp="not-a-real-timestamp")], schema=RAW_TRANSACTION_SCHEMA
    )
    _, invalid_df = split_valid_invalid(df)
    assert invalid_df.count() == 1
    assert "INVALID_TIMESTAMP" in invalid_df.collect()[0]["failure_reason"]


def test_invalid_transaction_type_is_quarantined(spark):
    df = spark.createDataFrame(
        [_row(transaction_type="TELEPORT_PAYMENT")], schema=RAW_TRANSACTION_SCHEMA
    )
    _, invalid_df = split_valid_invalid(df)
    assert invalid_df.count() == 1
    assert "INVALID_TRANSACTION_TYPE" in invalid_df.collect()[0]["failure_reason"]


def test_invalid_status_is_quarantined(spark):
    df = spark.createDataFrame([_row(status="UNKNOWN_STATE")], schema=RAW_TRANSACTION_SCHEMA)
    _, invalid_df = split_valid_invalid(df)
    assert invalid_df.count() == 1
    assert "INVALID_STATUS" in invalid_df.collect()[0]["failure_reason"]


def test_non_numeric_amount_is_quarantined(spark):
    df = spark.createDataFrame([_row(amount="abcdef")], schema=RAW_TRANSACTION_SCHEMA)
    _, invalid_df = split_valid_invalid(df)
    assert invalid_df.count() == 1
    assert "INVALID_AMOUNT" in invalid_df.collect()[0]["failure_reason"]


def test_missing_account_id_is_quarantined(spark):
    df = spark.createDataFrame([_row(account_id=None)], schema=RAW_TRANSACTION_SCHEMA)
    _, invalid_df = split_valid_invalid(df)
    assert invalid_df.count() == 1
    assert "MISSING_REQUIRED_FIELD" in invalid_df.collect()[0]["failure_reason"]


def test_mixed_batch_splits_correctly(spark):
    rows = [_row(transaction_id="TXN-OK"), _row(transaction_id="TXN-BAD", amount="-1")]
    df = spark.createDataFrame(rows, schema=RAW_TRANSACTION_SCHEMA)
    valid_df, invalid_df = split_valid_invalid(df)
    assert valid_df.count() == 1
    assert invalid_df.count() == 1
