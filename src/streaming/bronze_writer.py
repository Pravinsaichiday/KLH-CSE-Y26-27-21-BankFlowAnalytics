"""
Bronze layer writer: append-only raw valid transactions enriched with
pipeline ingestion metadata, written to Delta Lake with checkpointing.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src.common.config import settings
from src.common.logging_config import get_logger

logger = get_logger(__name__)


def enrich_for_bronze(valid_df: DataFrame) -> DataFrame:
    """Attach pipeline ingestion metadata required at the Bronze layer."""
    return (
        valid_df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("pipeline_source", F.lit("bankflow-streaming"))
    )


def write_bronze_stream(valid_df: DataFrame) -> StreamingQuery:
    """Start the Bronze Delta append-only stream sink, partitioned by date."""
    enriched = enrich_for_bronze(valid_df)

    checkpoint_path = str(settings.checkpoint_base_path / "bronze")
    output_path = str(settings.bronze_path)

    query = (
        enriched.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("ingestion_date")
        .start(output_path)
    )
    logger.info("Started Bronze Delta stream -> %s", output_path)
    return query
