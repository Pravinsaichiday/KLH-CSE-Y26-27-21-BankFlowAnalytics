"""
BankFlow Analytics - Silver-to-Gold Airflow DAG.

Strictly orchestrates batch Silver -> Gold loading and reporting. This DAG
does NOT process real-time streaming records — that is handled continuously
by the separate PySpark Structured Streaming job (scripts/run_streaming.py).

Task graph:
    start >> validate_silver >> load_dim_date
          >> [load_dim_account, load_dim_transaction_type, load_dim_channel, load_dim_location]
          >> load_fact_transactions >> calculate_kpis >> validate_gold >> end
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException

# Make `src` importable inside the Airflow container/environment.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "bankflow-data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

DATA_QUALITY_MIN_SUCCESS_RATE = 0.0  # sanity floor; adjust once baselines exist


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def _get_engine():
    from src.gold.sqlite_loader import get_engine, initialize_schema

    engine = get_engine()
    initialize_schema(engine)
    return engine


def task_validate_silver(**context) -> None:
    from src.common.config import settings

    silver_path = settings.silver_path
    if not silver_path.exists():
        raise AirflowFailException(
            f"Silver Delta path does not exist: {silver_path}. "
            "Has the streaming job run yet?"
        )
    logger.info("Silver path verified: %s", silver_path)


def task_extract_silver(**context) -> None:
    from src.gold.sqlite_loader import read_recent_silver_as_pandas
    from src.common.config import settings

    lookback_hours = settings.app["gold"]["batch_lookback_hours"]
    pdf = read_recent_silver_as_pandas(lookback_hours=lookback_hours)

    ti = context["ti"]
    # Airflow XCom isn't well-suited to large DataFrames; persist to a
    # scratch parquet file and pass the path instead.
    scratch_dir = REPO_ROOT / "data" / "_scratch"
    scratch_dir.mkdir(parents=True, exist_ok=True)
    scratch_path = scratch_dir / f"silver_extract_{context['ds_nodash']}_{context['ts_nodash']}.parquet"

    pdf.to_parquet(scratch_path, index=False)
    ti.xcom_push(key="silver_extract_path", value=str(scratch_path))
    ti.xcom_push(key="row_count", value=int(len(pdf)))
    logger.info("Extracted %d Silver rows -> %s", len(pdf), scratch_path)


def _load_extract(context) -> "object":
    import pandas as pd

    ti = context["ti"]
    path = ti.xcom_pull(task_ids="extract_silver", key="silver_extract_path")
    if not path or not Path(path).exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def task_load_dim_date(**context) -> None:
    import pandas as pd
    from src.gold.dimensions import load_dim_date

    pdf = _load_extract(context)
    engine = _get_engine()
    if pdf.empty:
        logger.warning("No Silver rows extracted; skipping dim_date load.")
        return

    ts = pd.to_datetime(pdf["event_timestamp"])
    load_dim_date(engine, ts.min().to_pydatetime(), ts.max().to_pydatetime())


def task_load_dim_account(**context) -> None:
    from src.gold.dimensions import load_dim_account

    pdf = _load_extract(context)
    engine = _get_engine()
    load_dim_account(engine, pdf)


def task_load_dim_transaction_type(**context) -> None:
    from src.gold.dimensions import load_dim_transaction_type

    pdf = _load_extract(context)
    engine = _get_engine()
    load_dim_transaction_type(engine, pdf)


def task_load_dim_channel(**context) -> None:
    from src.gold.dimensions import load_dim_channel

    pdf = _load_extract(context)
    engine = _get_engine()
    load_dim_channel(engine, pdf)


def task_load_dim_location(**context) -> None:
    from src.gold.dimensions import load_dim_location

    pdf = _load_extract(context)
    engine = _get_engine()
    load_dim_location(engine, pdf)


def task_load_fact_transactions(**context) -> None:
    from src.gold.facts import load_fact_transactions

    pdf = _load_extract(context)
    engine = _get_engine()
    inserted = load_fact_transactions(engine, pdf)
    context["ti"].xcom_push(key="rows_inserted", value=inserted)


def task_calculate_kpis(**context) -> None:
    from src.gold.metrics import recompute_all_metrics

    engine = _get_engine()
    result = recompute_all_metrics(engine)
    context["ti"].xcom_push(key="metrics_result", value=result)
    logger.info("KPI recompute result: %s", result)


def task_validate_gold(**context) -> None:
    from sqlalchemy import text

    engine = _get_engine()
    with engine.connect() as conn:
        orphaned_accounts = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM fact_transactions f
                LEFT JOIN dim_account a ON f.account_key = a.account_key
                WHERE a.account_key IS NULL
                """
            )
        ).scalar()

        inconsistent_flags = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM fact_transactions
                WHERE (is_anomaly = 1 AND anomaly_score < 5)
                   OR (is_anomaly = 0 AND anomaly_score >= 5)
                """
            )
        ).scalar()

        duplicate_txns = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM (
                    SELECT transaction_id FROM fact_transactions
                    GROUP BY transaction_id HAVING COUNT(*) > 1
                )
                """
            )
        ).scalar()

    failures = []
    if orphaned_accounts and orphaned_accounts > 0:
        failures.append(f"{orphaned_accounts} orphaned account FKs")
    if inconsistent_flags and inconsistent_flags > 0:
        failures.append(f"{inconsistent_flags} inconsistent anomaly flags")
    if duplicate_txns and duplicate_txns > 0:
        failures.append(f"{duplicate_txns} duplicate transaction_ids")

    if failures:
        raise AirflowFailException(
            "Gold layer data quality validation failed: " + "; ".join(failures)
        )
    logger.info("Gold layer data quality validation passed.")


def task_cleanup_scratch(**context) -> None:
    ti = context["ti"]
    path = ti.xcom_pull(task_ids="extract_silver", key="silver_extract_path")
    if path and Path(path).exists():
        Path(path).unlink()
        logger.info("Removed scratch extract file: %s", path)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="bankflow_gold_pipeline",
    description="BankFlow Analytics: Silver Delta -> Gold SQLite star schema + KPIs",
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bankflow", "gold", "batch"],
) as dag:

    start = EmptyOperator(task_id="start")

    validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=task_validate_silver,
    )

    extract_silver = PythonOperator(
        task_id="extract_silver",
        python_callable=task_extract_silver,
    )

    load_dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=task_load_dim_date,
    )

    load_dim_account = PythonOperator(
        task_id="load_dim_account",
        python_callable=task_load_dim_account,
    )
    load_dim_transaction_type = PythonOperator(
        task_id="load_dim_transaction_type",
        python_callable=task_load_dim_transaction_type,
    )
    load_dim_channel = PythonOperator(
        task_id="load_dim_channel",
        python_callable=task_load_dim_channel,
    )
    load_dim_location = PythonOperator(
        task_id="load_dim_location",
        python_callable=task_load_dim_location,
    )

    load_fact_transactions = PythonOperator(
        task_id="load_fact_transactions",
        python_callable=task_load_fact_transactions,
    )

    calculate_kpis = PythonOperator(
        task_id="calculate_kpis",
        python_callable=task_calculate_kpis,
    )

    validate_gold = PythonOperator(
        task_id="validate_gold",
        python_callable=task_validate_gold,
    )

    cleanup_scratch = PythonOperator(
        task_id="cleanup_scratch",
        python_callable=task_cleanup_scratch,
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    start >> validate_silver >> extract_silver >> load_dim_date
    load_dim_date >> [
        load_dim_account,
        load_dim_transaction_type,
        load_dim_channel,
        load_dim_location,
    ] >> load_fact_transactions
    load_fact_transactions >> calculate_kpis >> validate_gold >> cleanup_scratch >> end
