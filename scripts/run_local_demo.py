#!/usr/bin/env python3
"""
BankFlow Analytics - LOCAL DEMO PIPELINE (no Docker / Kafka / Spark / Airflow)

This is a pure-Python stand-in for the full streaming architecture, meant
for running the project end-to-end on a laptop without standing up the
whole infra stack. It reuses the same generator and Gold-layer modules as
the full pipeline (src/generator, src/gold) so the star schema and anomaly
semantics stay consistent with the production design.

Pipeline (all in-process, pandas-based):
    Generate synthetic transactions
        -> validate (split valid / quarantine)
        -> compute rolling-window anomaly features per account
        -> score anomalies (same weights/thresholds as config/anomaly.yaml)
        -> load Gold SQLite star schema (dims, facts, KPIs)

Usage:
    python scripts/run_local_demo.py --count 500 --anomaly-ratio 0.1
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pandas as pd

from src.common.config import settings
from src.common.logging_config import get_logger
from src.common.utils import parse_iso
from src.generator.transaction_generator import TransactionGenerator
from src.gold.sqlite_loader import get_engine, initialize_schema
from src.gold.dimensions import load_all_dimensions
from src.gold.facts import load_fact_transactions
from src.gold.metrics import recompute_all_metrics

logger = get_logger(__name__)

SCHEMA = settings.schema["schema"]
VALID_TYPES = set(SCHEMA["transaction_types"])
VALID_STATUSES = set(SCHEMA["statuses"])
REQUIRED_FIELDS = SCHEMA["required_fields"]

_ANOMALY_CFG = settings.anomaly["anomaly"]
_THRESH = _ANOMALY_CFG["thresholds"]
_WEIGHTS = _ANOMALY_CFG["scoring_weights"]
_IS_ANOMALY_THRESHOLD = _ANOMALY_CFG["is_anomaly_score_threshold"]


# ---------------------------------------------------------------------------
# 1. Validation (pandas equivalent of src/streaming/validation.py)
# ---------------------------------------------------------------------------
def validate(events: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid_rows, quarantine_rows = [], []

    for e in events:
        reasons = []

        missing = [f for f in REQUIRED_FIELDS if e.get(f) in (None, "")]
        if missing:
            reasons.append("MISSING_REQUIRED_FIELD")

        amount = e.get("amount")
        try:
            amount_f = float(amount)
            if amount_f <= 0:
                reasons.append("INVALID_AMOUNT")
        except (TypeError, ValueError):
            reasons.append("INVALID_AMOUNT")
            amount_f = None

        ts = e.get("timestamp")
        parsed_ts = None
        try:
            parsed_ts = parse_iso(ts) if ts else None
            if parsed_ts is None:
                reasons.append("INVALID_TIMESTAMP")
        except Exception:
            reasons.append("INVALID_TIMESTAMP")

        if e.get("transaction_type") not in VALID_TYPES:
            reasons.append("INVALID_TRANSACTION_TYPE")
        if e.get("status") not in VALID_STATUSES:
            reasons.append("INVALID_STATUS")

        if reasons:
            quarantine_rows.append({**e, "failure_reason": ",".join(reasons)})
        else:
            row = dict(e)
            row["amount"] = amount_f
            row["event_ts"] = parsed_ts
            valid_rows.append(row)

    valid_df = pd.DataFrame(valid_rows)
    quarantine_df = pd.DataFrame(quarantine_rows)
    return valid_df, quarantine_df


# ---------------------------------------------------------------------------
# 2. Rolling window features + anomaly scoring
#    (pandas equivalent of src/streaming/transformations.py + anomaly_detection.py)
# ---------------------------------------------------------------------------
def compute_anomaly_features(valid_df: pd.DataFrame) -> pd.DataFrame:
    if valid_df.empty:
        return valid_df

    df = valid_df.sort_values("event_ts").reset_index(drop=True)
    df["event_hour"] = df["event_ts"].apply(lambda t: t.hour)

    window_minutes = _THRESH["burst_window_minutes"]
    window = pd.Timedelta(minutes=window_minutes)

    account_mean = df.groupby("account_id")["amount"].transform("mean")
    df["account_rolling_mean_amount"] = account_mean

    txn_counts, failed_counts, distinct_devices, distinct_cities = [], [], [], []

    for account_id, group in df.groupby("account_id"):
        times = group["event_ts"]
        for idx, t in zip(group.index, times):
            in_window = group[(group["event_ts"] >= t - window) & (group["event_ts"] <= t + window)]
            txn_counts.append((idx, len(in_window)))
            failed_counts.append((idx, (in_window["status"] == "FAILED").sum()))
            distinct_devices.append((idx, in_window["device_id"].nunique()))
            distinct_cities.append((idx, in_window["city"].nunique()))

    df["window_txn_count"] = pd.Series(dict(txn_counts))
    df["window_failed_count"] = pd.Series(dict(failed_counts))
    df["window_distinct_devices"] = pd.Series(dict(distinct_devices))
    df["window_distinct_cities"] = pd.Series(dict(distinct_cities))

    df["high_amount_flag"] = df["amount"] > (
        df["account_rolling_mean_amount"] * _THRESH["high_amount_multiplier"]
    )
    df["burst_flag"] = df["window_txn_count"] > _THRESH["burst_transaction_limit"]
    df["failure_burst_flag"] = (df["status"] == "FAILED") & (
        df["window_failed_count"] > _THRESH["repeated_failures_limit"]
    )
    df["location_jump_flag"] = df["window_distinct_cities"] > 1
    df["new_device_flag"] = df["window_distinct_devices"] > 1
    df["unusual_hour_flag"] = (df["event_hour"] >= _THRESH["unusual_hour_start"]) & (
        df["event_hour"] < _THRESH["unusual_hour_end"]
    )

    df["anomaly_score"] = (
        df["high_amount_flag"].astype(int) * _WEIGHTS["high_amount"]
        + df["burst_flag"].astype(int) * _WEIGHTS["burst"]
        + df["failure_burst_flag"].astype(int) * _WEIGHTS["repeated_failures"]
        + df["location_jump_flag"].astype(int) * _WEIGHTS["location_jump"]
        + df["new_device_flag"].astype(int) * _WEIGHTS["new_device"]
        + df["unusual_hour_flag"].astype(int) * _WEIGHTS["unusual_hour"]
    )

    def _reasons(row):
        r = []
        if row["high_amount_flag"]:
            r.append("HIGH_AMOUNT")
        if row["burst_flag"]:
            r.append("TRANSACTION_BURST")
        if row["failure_burst_flag"]:
            r.append("REPEATED_FAILURES")
        if row["location_jump_flag"]:
            r.append("LOCATION_JUMP")
        if row["new_device_flag"]:
            r.append("NEW_DEVICE")
        if row["unusual_hour_flag"]:
            r.append("UNUSUAL_HOUR")
        return ",".join(r)

    df["anomaly_reasons"] = df.apply(_reasons, axis=1)
    df["is_anomaly"] = df["anomaly_score"] >= _IS_ANOMALY_THRESHOLD

    df["event_timestamp"] = df["event_ts"].apply(
        lambda t: t.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )

    return df


# ---------------------------------------------------------------------------
# 3. Local file outputs (stand-ins for Bronze/Silver/Quarantine Delta tables)
# ---------------------------------------------------------------------------
def write_local_layers(valid_scored_df: pd.DataFrame, quarantine_df: pd.DataFrame) -> None:
    bronze_dir = REPO_ROOT / "data" / "bronze_local"
    silver_dir = REPO_ROOT / "data" / "silver_local"
    quarantine_dir = REPO_ROOT / "data" / "quarantine_local"
    for d in (bronze_dir, silver_dir, quarantine_dir):
        d.mkdir(parents=True, exist_ok=True)

    if not valid_scored_df.empty:
        valid_scored_df.drop(columns=["event_ts"], errors="ignore").to_csv(
            bronze_dir / "transactions.csv", index=False
        )
        valid_scored_df.drop(columns=["event_ts"], errors="ignore").to_csv(
            silver_dir / "transactions.csv", index=False
        )
    if not quarantine_df.empty:
        quarantine_df.to_csv(quarantine_dir / "transactions.csv", index=False)

    logger.info(
        "Wrote local layers: bronze/silver=%d rows, quarantine=%d rows",
        len(valid_scored_df),
        len(quarantine_df),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Run the BankFlow local (no-infra) demo pipeline.")
    parser.add_argument("--count", type=int, default=500, help="Number of generator units to produce.")
    parser.add_argument("--anomaly-ratio", type=float, default=0.1, help="Probability of an anomaly batch.")
    parser.add_argument("--num-accounts", type=int, default=100, help="Synthetic account pool size.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    args = parser.parse_args()

    logger.info("Generating %d units (anomaly_ratio=%.2f)...", args.count, args.anomaly_ratio)
    generator = TransactionGenerator(
        num_accounts=args.num_accounts, anomaly_ratio=args.anomaly_ratio, seed=args.seed
    )
    events = generator.stream(count=args.count)
    logger.info("Generated %d raw events.", len(events))

    valid_df, quarantine_df = validate(events)
    logger.info("Validation: %d valid, %d quarantined.", len(valid_df), len(quarantine_df))

    scored_df = compute_anomaly_features(valid_df)
    if not scored_df.empty:
        logger.info(
            "Anomaly scoring: %d flagged ANOMALY (score >= %d) of %d valid rows.",
            int(scored_df["is_anomaly"].sum()),
            _IS_ANOMALY_THRESHOLD,
            len(scored_df),
        )

    write_local_layers(scored_df, quarantine_df)

    logger.info("Loading Gold SQLite star schema...")
    engine = get_engine()
    initialize_schema(engine)

    if not scored_df.empty:
        ts = pd.to_datetime(scored_df["event_timestamp"])
        load_all_dimensions(engine, scored_df, ts.min().to_pydatetime(), ts.max().to_pydatetime())
        inserted = load_fact_transactions(engine, scored_df)
        metrics_result = recompute_all_metrics(engine)
        print(f"\nDone. Inserted {inserted} fact rows. Metrics: {metrics_result}")
        print(f"Gold DB: {settings.gold_sqlite_path}")
    else:
        print("No valid rows were generated (unlucky run) — try again or increase --count.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
