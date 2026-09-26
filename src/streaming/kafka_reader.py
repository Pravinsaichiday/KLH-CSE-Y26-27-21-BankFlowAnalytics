"""
Spark Structured Streaming Kafka source reader for BankFlow Analytics.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import settings
from src.streaming.schema import RAW_TRANSACTION_SCHEMA


def build_spark_session() -> SparkSession:
    """
    Build a SparkSession configured with the Delta Lake extension and the
    Kafka + Delta packages required by this pipeline.
    """
    spark_cfg = settings.app["spark"]

    builder = (
        SparkSession.builder.appName(spark_cfg["app_name"])
        .master(spark_cfg["master"])
        .config("spark.sql.shuffle.partitions", spark_cfg["shuffle_partitions"])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_transaction_stream(spark: SparkSession) -> DataFrame:
    """
    Read the raw transaction Kafka topic as a streaming DataFrame, parse the
    JSON payload according to RAW_TRANSACTION_SCHEMA, and retain Kafka
    metadata columns (topic, partition, offset) for quarantine lineage.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.topics["transactions"])
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.col("topic"),
        F.col("partition").cast("int").alias("partition"),
        F.col("offset").cast("int").alias("offset"),
        F.from_json(F.col("value").cast("string"), RAW_TRANSACTION_SCHEMA).alias("data"),
    )

    return parsed.select("topic", "partition", "offset", "data.*")
