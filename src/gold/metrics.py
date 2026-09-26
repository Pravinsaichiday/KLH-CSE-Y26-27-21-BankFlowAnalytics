"""
Gold layer: KPI aggregate computation (daily_transaction_metrics,
hourly_transaction_metrics, account_risk_summary).

These tables are fully recomputed from fact_transactions for the affected
date range on every run, which keeps them simple and always consistent
(BankFlow's data volumes make a full recompute of the recent window cheap;
see sql/validation_queries.sql query #7 for the reconciliation check).
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.logging_config import get_logger

logger = get_logger(__name__)


def recompute_daily_metrics(engine: Engine) -> int:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM daily_transaction_metrics"))
        result = conn.execute(
            text(
                """
                INSERT INTO daily_transaction_metrics (
                    date_key, total_transactions, total_volume, success_count,
                    failed_count, success_rate, anomaly_count, anomaly_rate,
                    avg_transaction_value
                )
                SELECT
                    date_key,
                    COUNT(*) AS total_transactions,
                    SUM(amount) AS total_volume,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
                    CAST(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS REAL)
                        / NULLIF(COUNT(*), 0) AS success_rate,
                    SUM(is_anomaly) AS anomaly_count,
                    CAST(SUM(is_anomaly) AS REAL) / NULLIF(COUNT(*), 0) AS anomaly_rate,
                    AVG(amount) AS avg_transaction_value
                FROM fact_transactions
                GROUP BY date_key
                """
            )
        )
    logger.info("daily_transaction_metrics recomputed.")
    return result.rowcount or 0


def recompute_hourly_metrics(engine: Engine) -> int:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM hourly_transaction_metrics"))
        result = conn.execute(
            text(
                """
                INSERT INTO hourly_transaction_metrics (
                    date_key, hour_of_day, total_transactions, total_volume,
                    anomaly_count, failed_count
                )
                SELECT
                    date_key,
                    CAST(strftime('%H', event_timestamp) AS INTEGER) AS hour_of_day,
                    COUNT(*) AS total_transactions,
                    SUM(amount) AS total_volume,
                    SUM(is_anomaly) AS anomaly_count,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
                FROM fact_transactions
                GROUP BY date_key, hour_of_day
                """
            )
        )
    logger.info("hourly_transaction_metrics recomputed.")
    return result.rowcount or 0


def recompute_account_risk_summary(engine: Engine) -> int:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM account_risk_summary"))
        result = conn.execute(
            text(
                """
                INSERT INTO account_risk_summary (
                    account_key, total_transactions, total_volume, anomaly_count,
                    max_anomaly_score, avg_anomaly_score, last_transaction_ts, risk_band
                )
                SELECT
                    account_key,
                    COUNT(*) AS total_transactions,
                    SUM(amount) AS total_volume,
                    SUM(is_anomaly) AS anomaly_count,
                    MAX(anomaly_score) AS max_anomaly_score,
                    AVG(anomaly_score) AS avg_anomaly_score,
                    MAX(event_timestamp) AS last_transaction_ts,
                    CASE
                        WHEN MAX(anomaly_score) >= 5 THEN 'ANOMALY'
                        WHEN MAX(anomaly_score) >= 3 THEN 'WATCH'
                        ELSE 'NORMAL'
                    END AS risk_band
                FROM fact_transactions
                GROUP BY account_key
                """
            )
        )
    logger.info("account_risk_summary recomputed.")
    return result.rowcount or 0


def recompute_all_metrics(engine: Engine) -> dict:
    return {
        "daily": recompute_daily_metrics(engine),
        "hourly": recompute_hourly_metrics(engine),
        "account_risk": recompute_account_risk_summary(engine),
    }
