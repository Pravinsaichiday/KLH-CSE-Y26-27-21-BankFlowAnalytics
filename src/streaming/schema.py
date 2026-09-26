"""
Canonical Spark StructType definitions for raw Kafka JSON payloads and
downstream Bronze/Silver record shapes.
"""

from __future__ import annotations

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
)

# Raw payload schema as produced by the generator/producer. Fields are kept
# as StringType where a malformed producer payload could otherwise crash
# schema-on-read (e.g. amount arriving as a non-numeric string, or a
# missing/null transaction_id) — validation.py performs the actual typed
# checks and casts.
RAW_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("city", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("_injected_violation", StringType(), True),
    ]
)

# Typed schema used once a record has passed validation and casts.
TYPED_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("event_timestamp", StringType(), False),  # kept as string ISO + parsed ts column
        StructField("channel", StringType(), True),
        StructField("status", StringType(), False),
        StructField("merchant_category", StringType(), True),
        StructField("city", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("source", StringType(), True),
    ]
)

QUARANTINE_SCHEMA_EXTRA_FIELDS = StructType(
    [
        StructField("raw_record", StringType(), True),
        StructField("failure_reason", StringType(), True),
        StructField("validation_timestamp", StringType(), True),
        StructField("source_topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", IntegerType(), True),
    ]
)
