#!/usr/bin/env python3
"""
CLI: Run the PySpark Structured Streaming job (Kafka -> validation ->
Bronze/Silver Delta + Quarantine), with graceful shutdown and checkpoint
recovery (Spark's own checkpointing handles recovery automatically on
restart -- this script just needs to keep the driver process alive and
handle SIGINT/SIGTERM cleanly).

Usage:
    python scripts/run_streaming.py
    python scripts/run_streaming.py --await-termination-timeout 0   # run forever
"""

from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.common.logging_config import get_logger  # noqa: E402
from src.streaming.kafka_reader import build_spark_session, read_transaction_stream  # noqa: E402
from src.streaming.validation import split_valid_invalid  # noqa: E402
from src.streaming.bronze_writer import write_bronze_stream  # noqa: E402
from src.streaming.silver_writer import write_silver_stream  # noqa: E402
from src.streaming.quarantine import write_quarantine_stream  # noqa: E402

logger = get_logger(__name__)

_active_queries: List = []
_shutdown_requested = False


def _handle_signal(signum, frame) -> None:
    global _shutdown_requested
    logger.info("Received signal %s; requesting graceful shutdown...", signum)
    _shutdown_requested = True
    for query in _active_queries:
        try:
            query.stop()
        except Exception as exc:  # pragma: no cover
            logger.warning("Error stopping query %s: %s", getattr(query, "id", "?"), exc)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the BankFlow Structured Streaming pipeline.")
    parser.add_argument(
        "--await-termination-timeout",
        type=int,
        default=0,
        help="Milliseconds to wait for termination before returning. 0 = wait forever.",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("Building Spark session...")
    spark = build_spark_session()

    logger.info("Reading Kafka transaction stream...")
    raw_stream = read_transaction_stream(spark)

    valid_df, invalid_df = split_valid_invalid(raw_stream)

    logger.info("Starting Bronze, Silver, and Quarantine sinks...")
    bronze_query = write_bronze_stream(valid_df)
    silver_query = write_silver_stream(valid_df)
    quarantine_query = write_quarantine_stream(invalid_df)

    _active_queries.extend([bronze_query, silver_query, quarantine_query])

    logger.info("All streams started. Awaiting termination (Ctrl+C to stop gracefully)...")

    try:
        timeout = args.await_termination_timeout or None
        for query in list(_active_queries):
            if timeout:
                query.awaitTermination(timeout)
            else:
                query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; stopping streams...")
        for query in _active_queries:
            query.stop()
    finally:
        spark.stop()
        logger.info("Spark session stopped. Streaming job exited cleanly.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
