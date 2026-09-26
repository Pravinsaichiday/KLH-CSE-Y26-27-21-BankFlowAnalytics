"""
Gold layer orchestrator: reads recent Silver Delta records, initializes the
SQLite schema if needed, and drives dimension/fact/metric loads.

Designed to be called both from the Airflow DAG (airflow/dags/) and
standalone (e.g. `python -m src.gold.sqlite_loader`).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.common.config import settings, REPO_ROOT
from src.common.logging_config import get_logger
from src.gold.dimensions import load_all_dimensions
from src.gold.facts import load_fact_transactions
from src.gold.metrics import recompute_all_metrics

logger = get_logger(__name__)

SQL_DIR = REPO_ROOT / "sql"


def get_engine() -> Engine:
    db_path = settings.gold_sqlite_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{db_path}", future=True)


def initialize_schema(engine: Engine) -> None:
    """Run the DDL files idempotently (CREATE TABLE IF NOT EXISTS everywhere)."""
    ddl_files = ["create_dimensions.sql", "create_facts.sql", "create_metrics.sql"]
    with engine.begin() as conn:
        for filename in ddl_files:
            path = SQL_DIR / filename
            sql_text = path.read_text(encoding="utf-8")
            for statement in _split_sql_statements(sql_text):
                conn.execute(text(statement))
    logger.info("Gold SQLite schema initialized/verified.")


def _split_sql_statements(sql_text: str):
    """Naively split a .sql file on ';' terminators, skipping comments/blank lines."""
    statements = []
    buffer = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped == "":
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(buffer))
            buffer = []
    if buffer:
        statements.append("\n".join(buffer))
    return statements


def read_recent_silver_as_pandas(lookback_hours: int = 26) -> pd.DataFrame:
    """
    Read the Silver Delta table for the trailing `lookback_hours` window
    using Spark, then convert to pandas for the (comparatively small) Gold
    load. Falls back to an empty DataFrame if the Silver table doesn't exist
    yet (e.g. streaming hasn't produced any output).
    """
    silver_path = str(settings.silver_path)
    if not Path(silver_path).exists():
        logger.warning("Silver path does not exist yet: %s", silver_path)
        return pd.DataFrame()

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("BankFlowGoldExtract")
        .master(settings.app["spark"]["master"])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.format("delta").load(silver_path)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        recent = df.filter(F.col("event_ts") >= F.lit(cutoff))
        pdf = recent.toPandas()
    finally:
        spark.stop()

    logger.info("Read %d Silver rows from the trailing %d hours.", len(pdf), lookback_hours)
    return pdf


def run_gold_load(lookback_hours: Optional[int] = None) -> dict:
    """End-to-end Silver -> Gold load: schema init, dims, facts, metrics."""
    lookback_hours = lookback_hours or settings.app["gold"]["batch_lookback_hours"]

    engine = get_engine()
    initialize_schema(engine)

    silver_pdf = read_recent_silver_as_pandas(lookback_hours=lookback_hours)

    if silver_pdf.empty:
        logger.warning("No new Silver records found; skipping fact/dimension load.")
        return {"rows_read": 0, "rows_inserted": 0, "metrics": {}}

    min_ts = pd.to_datetime(silver_pdf["event_timestamp"]).min()
    max_ts = pd.to_datetime(silver_pdf["event_timestamp"]).max()

    load_all_dimensions(engine, silver_pdf, start_date=min_ts.to_pydatetime(), end_date=max_ts.to_pydatetime())
    inserted = load_fact_transactions(engine, silver_pdf)

    metrics_result = {}
    if settings.app["gold"]["kpi_refresh_on_load"]:
        metrics_result = recompute_all_metrics(engine)

    logger.info(
        "Gold load complete. rows_read=%d rows_inserted=%d metrics=%s",
        len(silver_pdf),
        inserted,
        metrics_result,
    )
    return {
        "rows_read": len(silver_pdf),
        "rows_inserted": inserted,
        "metrics": metrics_result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the BankFlow Silver-to-Gold load.")
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=None,
        help="Hours of Silver history to (re)scan for new records.",
    )
    args = parser.parse_args()
    run_gold_load(lookback_hours=args.lookback_hours)


if __name__ == "__main__":
    main()
