"""
Quarantine (dead-letter) handling for invalid transaction records.

Enriches invalid records with `raw_record`, `failure_reason` (already
computed in validation.py), `validation_timestamp`, `source_topic`,
`partition`, and `offset`, then writes them to a Delta table.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src.common.config import settings
from src.common.logging_config import get_logger

logger = get_logger(__name__)

ALL_RAW_COLUMNS = [
    "transaction_id",
    "account_id",
    "transaction_type",
    "amount",
    "currency",
    "timestamp",
    "channel",
    "status",
    "merchant_category",
    "city",
    "device_id",
    "source",
]


def enrich_for_quarantine(invalid_df: DataFrame) -> DataFrame:
    """Add dead-letter metadata columns to an invalid-records DataFrame."""
    present_cols = [c for c in ALL_RAW_COLUMNS if c in invalid_df.columns]

    raw_struct = F.to_json(F.struct(*[F.col(c) for c in present_cols]))

    enriched = invalid_df.withColumn("raw_record", raw_struct).withColumn(
        "validation_timestamp", F.current_timestamp().cast("string")
    )

    # `kafka_topic`, `partition`, `offset` are provided by the Kafka source
    # reader as `topic`, `partition`, `offset` columns when selected upstream.
    if "topic" in enriched.columns:
        enriched = enriched.withColumnRenamed("topic", "source_topic")
    else:
        enriched = enriched.withColumn("source_topic", F.lit(settings.topics["transactions"]))

    if "partition" not in enriched.columns:
        enriched = enriched.withColumn("partition", F.lit(None).cast("int"))
    if "offset" not in enriched.columns:
        enriched = enriched.withColumn("offset", F.lit(None).cast("int"))

    return enriched.select(
        "raw_record",
        "failure_reason",
        "validation_timestamp",
        "source_topic",
        "partition",
        "offset",
    )


def write_quarantine_stream(invalid_df: DataFrame) -> StreamingQuery:
    """Start (or append to) the quarantine Delta stream sink."""
    enriched = enrich_for_quarantine(invalid_df)

    checkpoint_path = str(settings.checkpoint_base_path / "quarantine")
    output_path = str(settings.quarantine_path)

    query = (
        enriched.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .start(output_path)
    )
    logger.info("Started quarantine Delta stream -> %s", output_path)
    return query
