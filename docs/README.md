# BankFlow Analytics

An end-to-end, fully synthetic real-time banking transaction data pipeline
and explainable anomaly-monitoring platform.

```
Python Generator -> Kafka -> PySpark Structured Streaming -> Delta Lake
(Bronze + Silver + Quarantine) -> Airflow -> SQLite (Gold Star Schema) -> Power BI
```

**All data is synthetic.** No real customer or banking data or PII is used
anywhere in this project. `is_anomaly` / `anomaly_score` represent
statistically/rule-based *unusual activity flagged for investigation* —
never a confirmed-fraud determination.

## Quick start

```bash
cp .env.example .env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt --break-system-packages   # or omit the flag in a venv

# 1. Bring up Kafka + Airflow
cd docker && docker compose up -d && cd ..

# 2. Create topics
python scripts/create_topics.py

# 3. Start the streaming job (long-running; run in its own terminal/pane)
python scripts/run_streaming.py

# 4. Start generating synthetic traffic
python scripts/run_generator.py --rate 10 --anomaly-ratio 0.1 --duration 600

# 5. Airflow will pick up bankflow_gold_pipeline every 15 minutes, or trigger manually:
docker compose -f docker/docker-compose.yml exec airflow-webserver \
    airflow dags trigger bankflow_gold_pipeline

# 6. Connect Power BI per dashboards/powerbi/README_POWERBI.md
```

## Repository layout

See `BankFlow_Analytics_Project_Specification.md` for the full spec this
repo implements. Key entry points:

- `src/generator/` — synthetic data + anomaly-pattern injection
- `src/kafka/` — topic management + idempotent producer
- `src/streaming/` — Structured Streaming: validation, quarantine,
  transformations, anomaly scoring, Bronze/Silver writers
- `src/gold/` — Silver -> Gold star-schema + KPI loaders (SQLite)
- `airflow/dags/bankflow_gold_pipeline.py` — batch orchestration only
- `scripts/` — CLI runbooks (`create_topics`, `run_generator`,
  `run_streaming`, `reset_data`)
- `dashboards/powerbi/README_POWERBI.md` — connection guide + DAX + layouts
- `tests/unit`, `tests/integration` — pytest suites

## Running tests

```bash
pytest tests/unit                 # no external services required
pytest tests/integration          # needs a temp SQLite (auto) + Kafka for the Kafka suite
```

## Important operational note

`src/kafka/` is a local package with the same import name as the
`kafka-python-ng` dependency it depends on (`import kafka`). Always run
commands from the **repository root** (not from inside `src/`) so Python's
import resolution finds the installed third-party `kafka` package rather
than shadowing it with the local `src/kafka` package. All scripts in
`scripts/` already insert the repo root at the front of `sys.path` for you.
