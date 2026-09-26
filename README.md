# BankFlow Analytics

**Course Code:** 24DEA3101 — FDE Team 21
**Team:** B Raghu Nandan (2420030058) · C Pravin Sai (2420030777) · J Hemanth (2420039805)
**Faculty Guide:** Dr. N. Shirisha, Associate Professor

BankFlow Analytics is a real-time banking transaction analytics and anomaly monitoring pipeline. It ingests, validates, enriches, and analyzes synthetic transaction data end-to-end, and presents the results as a queryable business-intelligence warehouse.

All data used in this project is synthetically generated — no real customer or banking data is used anywhere in this repository.

---

## Project Overview

Digital banking platforms generate a continuous stream of transaction data through activities such as online payments, money transfers, ATM withdrawals, deposits, and bill payments. This project builds a pipeline that captures and processes such transaction data in real time, organizing it through a Medallion Architecture, and flags unusual activity using an explainable, rule-based anomaly detection engine.

The pipeline includes fault-tolerant data handling, a dedicated quarantine layer for invalid records, and automated batch orchestration for loading a Gold-layer data warehouse, which is finally visualized through Power BI dashboards.

---

## Pipeline Flow

```
Transaction Generator (Python)
        ↓
Apache Kafka
        ↓
PySpark Structured Streaming
        ↓
Validation & Quarantine
        ↓
Bronze Delta Layer  →  Silver Delta Layer
        ↓
Apache Airflow (Batch Orchestration)
        ↓
SQLite Gold Layer (Star Schema)
        ↓
Power BI Dashboard
```

---

## Repository Structure

```
BankFlowAnalytics/
│
├── src/
│   └── Project source code — generator, Kafka producer/topics, Spark streaming
│       (validation, quarantine, transformations, anomaly detection), Gold-layer loaders
│
├── data/
│   └── Populated Gold SQLite database and sample layer output (Bronze/Silver/Quarantine)
│
├── docs/
│   └── Project specification, system architecture diagram and notebook, Power BI guide
│
├── reports/
│   └── Review presentations (Project Review 1, 2, 3)
│
├── results/
│   └── Screenshots evidencing the pipeline running end-to-end
│
├── config/
│   └── Pipeline configuration — app, Kafka, anomaly thresholds, schema
│
├── scripts/
│   └── CLI runbooks — create topics, run generator, run streaming, reset data
│
├── sql/
│   └── Gold layer DDL — dimension tables, fact table, KPI metrics, validation queries
│
├── airflow/
│   └── DAG for Silver → Gold batch orchestration
│
├── docker/
│   └── docker-compose.yml for Kafka and Airflow
│
├── tests/
│   └── Unit and integration test suites
│
└── README.md
```

Dataset files and configuration containing sensitive information are excluded from version control via `.gitignore`.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Generation | Python, Faker |
| Ingestion | Apache Kafka |
| Stream Processing | PySpark Structured Streaming |
| Storage | Delta Lake (Bronze, Silver, Quarantine) |
| Orchestration | Apache Airflow |
| Data Warehouse | SQLite (Star Schema) |
| Visualization | Power BI |
| Containerization | Docker, Docker Compose |

---

## Getting Started

Clone the repository:

```
git clone https://github.com/Pravinsaichiday/KLH-CSE-Y26-27-21-BankFlowAnalytics.git
cd KLH-CSE-Y26-27-21-BankFlowAnalytics
```

Set up the environment:

```
cp .env.example .env
pip install -r requirements.txt
```

Run the pipeline (containerized):

```
# 1. Start Kafka
cd docker && docker compose -p bankflow up -d kafka kafka-ui && cd ..

# 2. Build the runtime image
docker build -t bankflow-runtime -f Dockerfile.runtime .

# 3. Create Kafka topics
docker run --rm --network bankflow_default -v "$(pwd):/app" -w /app -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 bankflow-runtime python scripts/create_topics.py

# 4. Start the streaming job
docker run -d --name bankflow-streaming --network bankflow_default -v "$(pwd):/app" -w /app -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 bankflow-runtime python scripts/run_streaming.py

# 5. Generate synthetic transactions
docker run --rm --network bankflow_default -v "$(pwd):/app" -w /app -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 bankflow-runtime python scripts/run_generator.py --rate 10 --anomaly-ratio 0.1 --duration 60

# 6. Load Silver to Gold
docker run --rm --network bankflow_default -v "$(pwd):/app" -w /app bankflow-runtime python -m src.gold.sqlite_loader
```

Browse the Gold database:

```
docker run --rm -p 8001:8001 -v "$(pwd)/data:/data" -w /data python:3.11-slim bash -c "pip install -q datasette && datasette bankflow_gold.db --host 0.0.0.0 --port 8001"
```

Run the tests:

```
pytest tests/unit
pytest tests/integration
```

---

## Documentation

- `docs/` — full technical specification, architecture diagram and notebook, Power BI dashboard guide
- `reports/` — project review presentations
- `results/` — pipeline execution evidence