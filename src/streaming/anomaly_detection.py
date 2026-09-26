"""
Explainable, rule/statistical-based anomaly scoring.

IMPORTANT: An `is_anomaly` flag here means "unusual activity that warrants
investigation" — it is NOT a fraud determination. Scores and reasons are
fully deterministic and derived from externalized thresholds in
config/anomaly.yaml so every flag can be explained to an analyst.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import settings

_CFG = settings.anomaly["anomaly"]
_THRESH = _CFG["thresholds"]
_WEIGHTS = _CFG["scoring_weights"]
_IS_ANOMALY_THRESHOLD = _CFG["is_anomaly_score_threshold"]

HIGH_AMOUNT_MULTIPLIER = _THRESH["high_amount_multiplier"]
BURST_TRANSACTION_LIMIT = _THRESH["burst_transaction_limit"]
REPEATED_FAILURES_LIMIT = _THRESH["repeated_failures_limit"]
UNUSUAL_HOUR_START = _THRESH["unusual_hour_start"]
UNUSUAL_HOUR_END = _THRESH["unusual_hour_end"]

W_HIGH_AMOUNT = _WEIGHTS["high_amount"]
W_BURST = _WEIGHTS["burst"]
W_REPEATED_FAILURES = _WEIGHTS["repeated_failures"]
W_LOCATION_JUMP = _WEIGHTS["location_jump"]
W_NEW_DEVICE = _WEIGHTS["new_device"]
W_UNUSUAL_HOUR = _WEIGHTS["unusual_hour"]


def add_risk_flags(df: DataFrame) -> DataFrame:
    """
    Attach individual boolean risk-signal flags. Expects the DataFrame to
    already carry `window_txn_count`, `window_failed_count`,
    `window_distinct_cities`, `window_distinct_devices`, `event_hour`, and
    (optionally) `account_rolling_mean_amount` from upstream transformations.
    """
    has_rolling_mean = "account_rolling_mean_amount" in df.columns

    df = df.withColumn(
        "high_amount_flag",
        (F.col("amount") > (F.coalesce(F.col("account_rolling_mean_amount"), F.col("amount") / 2) * HIGH_AMOUNT_MULTIPLIER))
        if has_rolling_mean
        else F.lit(False),
    )

    df = df.withColumn(
        "burst_flag", F.col("window_txn_count") > F.lit(BURST_TRANSACTION_LIMIT)
    )

    df = df.withColumn(
        "failure_burst_flag",
        (F.col("status") == "FAILED") & (F.col("window_failed_count") > F.lit(REPEATED_FAILURES_LIMIT)),
    )

    df = df.withColumn(
        "location_jump_flag",
        F.col("window_distinct_cities") > F.lit(1),
    )

    df = df.withColumn(
        "new_device_flag",
        F.col("window_distinct_devices") > F.lit(1),
    )

    df = df.withColumn(
        "unusual_hour_flag",
        (F.col("event_hour") >= F.lit(UNUSUAL_HOUR_START)) & (F.col("event_hour") < F.lit(UNUSUAL_HOUR_END)),
    )

    return df


def compute_anomaly_score(df: DataFrame) -> DataFrame:
    """Combine risk flags into a deterministic weighted score + reasons string."""
    df = df.withColumn(
        "anomaly_score",
        (F.col("high_amount_flag").cast("int") * F.lit(W_HIGH_AMOUNT))
        + (F.col("burst_flag").cast("int") * F.lit(W_BURST))
        + (F.col("failure_burst_flag").cast("int") * F.lit(W_REPEATED_FAILURES))
        + (F.col("location_jump_flag").cast("int") * F.lit(W_LOCATION_JUMP))
        + (F.col("new_device_flag").cast("int") * F.lit(W_NEW_DEVICE))
        + (F.col("unusual_hour_flag").cast("int") * F.lit(W_UNUSUAL_HOUR)),
    )

    reasons_array = F.array(
        F.when(F.col("high_amount_flag"), F.lit("HIGH_AMOUNT")),
        F.when(F.col("burst_flag"), F.lit("TRANSACTION_BURST")),
        F.when(F.col("failure_burst_flag"), F.lit("REPEATED_FAILURES")),
        F.when(F.col("location_jump_flag"), F.lit("LOCATION_JUMP")),
        F.when(F.col("new_device_flag"), F.lit("NEW_DEVICE")),
        F.when(F.col("unusual_hour_flag"), F.lit("UNUSUAL_HOUR")),
    )
    non_null_reasons = F.filter(reasons_array, lambda x: x.isNotNull())

    df = df.withColumn("anomaly_reasons", F.concat_ws(",", non_null_reasons))
    df = df.withColumn("is_anomaly", F.col("anomaly_score") >= F.lit(_IS_ANOMALY_THRESHOLD))

    df = df.withColumn(
        "anomaly_band",
        F.when(F.col("anomaly_score") >= F.lit(_IS_ANOMALY_THRESHOLD), F.lit("ANOMALY"))
        .when(F.col("anomaly_score") >= F.lit(3), F.lit("WATCH"))
        .otherwise(F.lit("NORMAL")),
    )

    return df


def apply_anomaly_detection(df: DataFrame) -> DataFrame:
    """Full pipeline stage: risk flags -> weighted score -> explainable reasons."""
    df = add_risk_flags(df)
    df = compute_anomaly_score(df)
    return df
