"""
Unit tests for src/streaming/anomaly_detection.py — verifies risk flags,
weighted scoring, and the is_anomaly threshold against config/anomaly.yaml.
"""

from __future__ import annotations

from pyspark.sql import Row

from src.streaming.anomaly_detection import apply_anomaly_detection
from src.common.config import settings

_CFG = settings.anomaly["anomaly"]
_WEIGHTS = _CFG["scoring_weights"]
_THRESHOLD = _CFG["is_anomaly_score_threshold"]


def _make_row(**overrides):
    base = dict(
        transaction_id="TXN-1",
        account_id="ACC1",
        amount=100.0,
        status="SUCCESS",
        event_hour=12,
        window_txn_count=1,
        window_total_amount=100.0,
        window_failed_count=0,
        window_distinct_devices=1,
        window_distinct_cities=1,
        account_rolling_mean_amount=100.0,
    )
    base.update(overrides)
    return Row(**base)


def test_normal_transaction_scores_zero(spark):
    df = spark.createDataFrame([_make_row()])
    result = apply_anomaly_detection(df).collect()[0]
    assert result["anomaly_score"] == 0
    assert result["is_anomaly"] is False
    assert result["anomaly_band"] == "NORMAL"


def test_high_amount_flag_triggers_score(spark):
    df = spark.createDataFrame(
        [_make_row(amount=1000.0, account_rolling_mean_amount=100.0)]
    )
    result = apply_anomaly_detection(df).collect()[0]
    assert result["high_amount_flag"] is True
    assert result["anomaly_score"] >= _WEIGHTS["high_amount"]
    assert "HIGH_AMOUNT" in result["anomaly_reasons"]


def test_burst_flag_triggers_score(spark):
    df = spark.createDataFrame([_make_row(window_txn_count=15)])
    result = apply_anomaly_detection(df).collect()[0]
    assert result["burst_flag"] is True
    assert "TRANSACTION_BURST" in result["anomaly_reasons"]


def test_failure_burst_flag_requires_failed_status(spark):
    df = spark.createDataFrame(
        [_make_row(status="FAILED", window_failed_count=5)]
    )
    result = apply_anomaly_detection(df).collect()[0]
    assert result["failure_burst_flag"] is True
    assert "REPEATED_FAILURES" in result["anomaly_reasons"]


def test_failure_burst_flag_false_if_not_failed_status(spark):
    df = spark.createDataFrame(
        [_make_row(status="SUCCESS", window_failed_count=5)]
    )
    result = apply_anomaly_detection(df).collect()[0]
    assert result["failure_burst_flag"] is False


def test_location_jump_flag(spark):
    df = spark.createDataFrame([_make_row(window_distinct_cities=2)])
    result = apply_anomaly_detection(df).collect()[0]
    assert result["location_jump_flag"] is True
    assert "LOCATION_JUMP" in result["anomaly_reasons"]


def test_unusual_hour_flag(spark):
    df = spark.createDataFrame([_make_row(event_hour=2)])
    result = apply_anomaly_detection(df).collect()[0]
    assert result["unusual_hour_flag"] is True
    assert "UNUSUAL_HOUR" in result["anomaly_reasons"]


def test_combined_flags_meet_anomaly_threshold(spark):
    df = spark.createDataFrame(
        [
            _make_row(
                amount=2000.0,
                account_rolling_mean_amount=100.0,  # high_amount: +2
                window_txn_count=20,                 # burst: +2
                status="FAILED",
                window_failed_count=5,                # repeated_failures: +2
            )
        ]
    )
    result = apply_anomaly_detection(df).collect()[0]
    assert result["anomaly_score"] >= _THRESHOLD
    assert result["is_anomaly"] is True
    assert result["anomaly_band"] == "ANOMALY"


def test_watch_band_between_three_and_four(spark):
    # high_amount(2) + unusual_hour(1) = 3 -> WATCH band
    df = spark.createDataFrame(
        [
            _make_row(
                amount=2000.0,
                account_rolling_mean_amount=100.0,
                event_hour=2,
            )
        ]
    )
    result = apply_anomaly_detection(df).collect()[0]
    assert 3 <= result["anomaly_score"] <= 4
    assert result["anomaly_band"] == "WATCH"
    assert result["is_anomaly"] is False
