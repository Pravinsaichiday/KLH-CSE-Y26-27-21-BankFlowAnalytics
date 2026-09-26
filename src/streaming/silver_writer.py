"""
Silver layer writer: cleaned, typed, deduplicated transactions enriched with
derived temporal/window features and explainable anomaly flags/scores,
written to Delta Lake with checkpointing.

Pipeline order for the Silver micro-batch:
    watermark -> dedup -> time features -> rolling window aggregates
    -> join stats back onto rows -> anomaly detection -> write
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src.common.config import settings
from src.common.logging_config import get_logger
from src.streaming.transformations import (
    apply_watermark,
    deduplicate,
    add_time_features,
    compute_rolling_window_aggregates,
    join_transaction_with_window_stats,
)
from src.streaming.anomaly_detection import apply_anomaly_detection

logger = get_logger(__name__)

SILVER_OUTPUT_COLUMNS = [
    "transaction_id",
    "account_id",
    "transaction_type",
    "amount",
    "currency",
    "event_timestamp",
    "event_ts",
    "event_date",
    "event_hour",
    "channel",
    "status",
    "merchant_category",
    "city",
    "device_id",
    "source",
    "window_txn_count",
    "window_total_amount",
    "window_failed_count",
    "window_distinct_devices",
    "window_distinct_cities",
    "high_amount_flag",
    "burst_flag",
    "failure_burst_flag",
    "location_jump_flag",
    "new_device_flag",
    "unusual_hour_flag",
    "anomaly_score",
    "anomaly_band",
    "is_anomaly",
    "anomaly_reasons",
]


def build_silver_frame(valid_df: DataFrame) -> DataFrame:
    """
    Apply the full Silver transformation chain to a validated transactions
    DataFrame (already typed by validation.split_valid_invalid).
    """
    watermarked = apply_watermark(valid_df)
    deduped = deduplicate(watermarked)
    with_time = add_time_features(deduped)

    window_stats = compute_rolling_window_aggregates(with_time)
    joined = join_transaction_with_window_stats(with_time, window_stats)

    scored = apply_anomaly_detection(joined)

    existing_cols = [c for c in SILVER_OUTPUT_COLUMNS if c in scored.columns]
    return scored.select(*existing_cols)


def write_silver_stream(valid_df: DataFrame) -> StreamingQuery:
    """Start the Silver Delta stream sink using `foreachBatch` so we can run
    the stateful window aggregation + join per micro-batch (these operations
    mix streaming-streaming join semantics that are simplest to reason about
    per-batch for this analytical, not low-latency, use case)."""

    checkpoint_path = str(settings.checkpoint_base_path / "silver")
    output_path = str(settings.silver_path)

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            logger.debug("Silver micro-batch %d is empty, skipping.", batch_id)
            return

        silver_df = build_silver_frame(batch_df)
        (
            silver_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("event_date")
            .save(output_path)
        )
        logger.info(
            "Silver micro-batch %d written: %d rows -> %s",
            batch_id,
            silver_df.count(),
            output_path,
        )

    query = (
        valid_df.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(_process_batch)
        .start()
    )
    logger.info("Started Silver Delta stream (foreachBatch) -> %s", output_path)
    return query
