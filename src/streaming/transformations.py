"""
Core Structured Streaming transformation logic: watermarking, dedup, and
rolling window feature engineering used as input to anomaly scoring.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import settings

WATERMARK_DURATION = settings.app["spark"]["watermark_duration"]  # e.g. "10 minutes"


def apply_watermark(valid_df: DataFrame) -> DataFrame:
    """Apply the event-time watermark required for stateful streaming ops."""
    return valid_df.withWatermark("event_ts", WATERMARK_DURATION)


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate on `transaction_id` within the watermark window using
    Structured Streaming's native dropDuplicatesWithinWatermark-style
    semantics (dropDuplicates after withWatermark achieves this in
    Spark 3.5, scoped to the watermark retention window).
    """
    return df.dropDuplicates(["transaction_id"])


def add_time_features(df: DataFrame) -> DataFrame:
    """Add derived temporal features used by anomaly detection rules."""
    return (
        df.withColumn("event_hour", F.hour("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_minute", F.minute("event_ts"))
    )


def compute_rolling_window_aggregates(df: DataFrame) -> DataFrame:
    """
    Compute rolling aggregate features per account over a sliding event-time
    window (5-minute window, 1-minute slide): transaction count, total
    amount, failed count, and distinct device count. These are joined back
    onto the original (deduplicated, watermarked) stream by transaction_id
    so each transaction row carries its own contextual window stats.
    """
    windowed = (
        df.groupBy(
            F.window(F.col("event_ts"), "5 minutes", "1 minute"),
            F.col("account_id"),
        )
        .agg(
            F.count("*").alias("window_txn_count"),
            F.sum("amount").alias("window_total_amount"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias(
                "window_failed_count"
            ),
            F.approx_count_distinct("device_id").alias("window_distinct_devices"),
            F.approx_count_distinct("city").alias("window_distinct_cities"),
        )
        .select(
            F.col("account_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "window_txn_count",
            "window_total_amount",
            "window_failed_count",
            "window_distinct_devices",
            "window_distinct_cities",
        )
    )
    return windowed


def join_transaction_with_window_stats(
    txn_df: DataFrame, window_df: DataFrame
) -> DataFrame:
    """
    Join each transaction to the rolling window row whose interval contains
    its event time, for the same account.
    """
    joined = txn_df.alias("t").join(
        window_df.alias("w"),
        (F.col("t.account_id") == F.col("w.account_id"))
        & (F.col("t.event_ts") >= F.col("w.window_start"))
        & (F.col("t.event_ts") < F.col("w.window_end")),
        how="left",
    )
    return joined.select(
        "t.*",
        F.coalesce(F.col("w.window_txn_count"), F.lit(1)).alias("window_txn_count"),
        F.coalesce(F.col("w.window_total_amount"), F.col("t.amount")).alias(
            "window_total_amount"
        ),
        F.coalesce(F.col("w.window_failed_count"), F.lit(0)).alias("window_failed_count"),
        F.coalesce(F.col("w.window_distinct_devices"), F.lit(1)).alias(
            "window_distinct_devices"
        ),
        F.coalesce(F.col("w.window_distinct_cities"), F.lit(1)).alias(
            "window_distinct_cities"
        ),
    )


def compute_account_rolling_mean(historical_df: DataFrame) -> DataFrame:
    """
    Compute a per-account rolling mean transaction amount over the trailing
    50 transactions (batch-oriented helper, typically applied to a recent
    slice of Silver/Bronze data rather than the live micro-batch, since
    unbounded window functions aren't supported directly on streaming
    DataFrames). Used by anomaly_detection.py's high-amount rule.
    """
    w = (
        Window.partitionBy("account_id")
        .orderBy("event_ts")
        .rowsBetween(-50, -1)
    )
    return historical_df.withColumn("account_rolling_mean_amount", F.avg("amount").over(w))
