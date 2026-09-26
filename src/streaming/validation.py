"""
Validation logic for raw transaction records read from Kafka.

Splits an incoming micro-batch DataFrame into (valid, invalid) DataFrames.
Valid records are cast to proper types for Bronze/Silver; invalid records
carry a `failure_reason` for routing to quarantine.
"""

from __future__ import annotations

from typing import Tuple

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F

from src.common.config import settings

SCHEMA = settings.schema["schema"]
VALID_TRANSACTION_TYPES = SCHEMA["transaction_types"]
VALID_STATUSES = SCHEMA["statuses"]
REQUIRED_FIELDS = SCHEMA["required_fields"]

# A conservative ISO-8601 pattern; actual parseability is re-checked via
# to_timestamp producing a non-null value.
_ISO_TS_REGEX = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$"


def _null_or_empty(col: Column) -> Column:
    return col.isNull() | (F.trim(col) == "")


def build_validation_flags(df: DataFrame) -> DataFrame:
    """Attach boolean flag columns describing every validation rule outcome."""

    parsed_amount = F.col("amount").cast("double")
    parsed_ts = F.to_timestamp(F.col("timestamp"))

    return (
        df.withColumn("_amount_numeric", parsed_amount)
        .withColumn("_ts_parsed", parsed_ts)
        .withColumn(
            "_missing_required_field",
            F.array_contains(
                F.array([F.lit(f) for f in REQUIRED_FIELDS]),
                F.lit(None).cast("string"),
            )
            | _null_or_empty(F.col("transaction_id"))
            | _null_or_empty(F.col("account_id"))
            | _null_or_empty(F.col("transaction_type"))
            | F.col("amount").isNull()
            | _null_or_empty(F.col("timestamp"))
            | _null_or_empty(F.col("status")),
        )
        .withColumn(
            "_invalid_amount",
            F.col("_amount_numeric").isNull() | (F.col("_amount_numeric") <= 0),
        )
        .withColumn("_invalid_timestamp", F.col("_ts_parsed").isNull())
        .withColumn(
            "_invalid_transaction_type",
            ~F.col("transaction_type").isin(VALID_TRANSACTION_TYPES),
        )
        .withColumn("_invalid_status", ~F.col("status").isin(VALID_STATUSES))
    )


def _failure_reason_col() -> Column:
    reasons = F.array(
        F.when(F.col("_missing_required_field"), F.lit("MISSING_REQUIRED_FIELD")),
        F.when(F.col("_invalid_amount"), F.lit("INVALID_AMOUNT")),
        F.when(F.col("_invalid_timestamp"), F.lit("INVALID_TIMESTAMP")),
        F.when(F.col("_invalid_transaction_type"), F.lit("INVALID_TRANSACTION_TYPE")),
        F.when(F.col("_invalid_status"), F.lit("INVALID_STATUS")),
    )
    non_null_reasons = F.filter(reasons, lambda x: x.isNotNull())
    return F.concat_ws(",", non_null_reasons)


def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Returns (valid_df, invalid_df).

    valid_df: typed & ready for Bronze/Silver, with the validation helper
              columns dropped.
    invalid_df: original raw columns + `failure_reason`, ready for the
                quarantine writer to enrich further.
    """
    flagged = build_validation_flags(df)

    is_invalid = (
        F.col("_missing_required_field")
        | F.col("_invalid_amount")
        | F.col("_invalid_timestamp")
        | F.col("_invalid_transaction_type")
        | F.col("_invalid_status")
    )

    invalid_df = (
        flagged.filter(is_invalid)
        .withColumn("failure_reason", _failure_reason_col())
        .drop(
            "_amount_numeric",
            "_ts_parsed",
            "_missing_required_field",
            "_invalid_amount",
            "_invalid_timestamp",
            "_invalid_transaction_type",
            "_invalid_status",
        )
    )

    valid_df = (
        flagged.filter(~is_invalid)
        .withColumn("amount", F.col("_amount_numeric"))
        .withColumn("event_timestamp", F.date_format(F.col("_ts_parsed"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .withColumn("event_ts", F.col("_ts_parsed"))
        .drop(
            "_amount_numeric",
            "_ts_parsed",
            "_missing_required_field",
            "_invalid_amount",
            "_invalid_timestamp",
            "_invalid_transaction_type",
            "_invalid_status",
            "timestamp",
        )
    )

    return valid_df, invalid_df
