# BankFlow Analytics

## Real-Time Banking Transaction Analytics & Anomaly Monitoring Platform

**Repository:** `bankflow-analytics`\
**Project type:** Real-time data engineering + business intelligence
platform\
**Domain:** Banking transaction analytics and anomaly monitoring\
**Language:** Python\
**Core pipeline:** Python -\> Apache Kafka -\> PySpark Structured
Streaming -\> Delta Lake -\> Apache Airflow -\> SQLite -\> Power BI\
**Architecture:** Event-driven streaming + Medallion Architecture\
**Data policy:** Synthetic data only. Never use real banking/customer
data.

------------------------------------------------------------------------

# 1. Project Overview

BankFlow Analytics is an end-to-end real-time banking transaction data
pipeline. It continuously generates or ingests synthetic banking
transactions such as transfers, UPI/online payments, ATM withdrawals,
deposits, bill payments, refunds and failed transactions.

The events are published to Apache Kafka. PySpark Structured Streaming
consumes the stream and performs schema validation, data-quality checks,
transformations, feature calculation and explainable anomaly monitoring.
Invalid records are isolated in a Quarantine Layer instead of stopping
the main pipeline.

Valid data follows a Medallion Architecture:

``` text
Bronze -> Silver -> Gold
```

Bronze and Silver are stored using Delta Lake. Apache Airflow
orchestrates the downstream Silver-to-Gold workflow. The Gold layer is
stored in SQLite using a Star Schema. Power BI reads the Gold data and
provides dashboards for transaction activity, value, success/failure
rates, account activity and anomaly indicators.

This is an **anomaly monitoring platform**, not a production
fraud-prevention system. An anomaly means unusual behavior that should
be investigated; it does not prove fraud.

------------------------------------------------------------------------

# 2. One-Sentence Definition

> BankFlow Analytics continuously streams banking transactions through
> Kafka, processes and validates them with PySpark, stores them through
> a Bronze/Silver/Gold architecture, orchestrates analytical
> transformations with Airflow, and presents transaction and anomaly
> insights through Power BI.

------------------------------------------------------------------------

# 3. Problem Statement

Banking systems produce large numbers of transactions continuously.
Batch-only reporting can delay visibility into current transaction
behavior. At the same time, malformed records can damage downstream
processing, and unusual transaction patterns can be difficult to
identify quickly.

The project solves this by providing:

-   Continuous transaction ingestion
-   Real-time/low-latency stream processing
-   Data validation
-   Quarantine handling
-   Transaction analytics
-   Explainable anomaly monitoring
-   Reliable layered storage
-   Automated Gold-layer transformation
-   Business intelligence dashboards

------------------------------------------------------------------------

# 4. Objectives

1.  Generate realistic synthetic banking transactions.
2.  Stream transactions continuously using Kafka.
3.  Process the stream using PySpark Structured Streaming.
4.  Validate required fields and business rules.
5.  Route invalid records to a Quarantine/Dead Letter layer.
6.  Store Bronze and Silver data using Delta Lake.
7.  Calculate streaming transaction features.
8.  Detect unusual transaction patterns using configurable
    rules/statistical features.
9.  Orchestrate downstream transformations using Airflow.
10. Build a Gold Star Schema in SQLite.
11. Create Power BI dashboards.
12. Test throughput, latency, data quality and failure recovery.

------------------------------------------------------------------------

# 5. Scope

## Included

-   Synthetic data generation
-   Kafka streaming
-   PySpark Structured Streaming
-   Schema validation
-   Quarantine layer
-   Bronze/Silver/Gold architecture
-   Delta Lake
-   Streaming anomaly monitoring
-   Airflow orchestration
-   SQLite Gold database
-   Star Schema
-   Power BI dashboards
-   Logging
-   Testing
-   Local/Docker development

## Not Included Unless Explicitly Added

-   Real bank integration
-   Real customer PII
-   Real account/card/UPI credentials
-   Actual money movement
-   Automatic account blocking
-   Production fraud prevention
-   Regulatory certification
-   Guaranteed fraud detection
-   Production cloud deployment

------------------------------------------------------------------------

# 6. Architecture

``` text
             +----------------------+
             | Python Transaction   |
             | Generator / Source   |
             +----------+-----------+
                        |
                        v
             +----------------------+
             | Apache Kafka         |
             | bank_transactions    |
             +----------+-----------+
                        |
                        v
             +----------------------+
             | PySpark Structured   |
             | Streaming            |
             +----+------------+----+
                  |            |
               Valid        Invalid
                  |            |
                  v            v
          +-------------+  +------------+
          | Bronze      |  | Quarantine |
          | Delta       |  | Layer      |
          +------+------+  +------------+
                 |
                 v
          +-------------+
          | Silver      |
          | Delta       |
          +------+------+
                 |
                 v
          +-------------+
          | Airflow DAG |
          +------+------+
                 |
                 v
          +-------------+
          | SQLite Gold |
          | Star Schema |
          +------+------+
                 |
                 v
          +-------------+
          | Power BI    |
          +-------------+
```

The responsibility of each stage is deliberately separated:

  Component     Responsibility
  ------------- ------------------------------
  Python        Generate synthetic events
  Kafka         Stream/buffer events
  PySpark       Process and transform events
  Quarantine    Isolate invalid records
  Delta Lake    Store Bronze/Silver data
  Airflow       Orchestrate workflows
  SQLite        Store Gold analytical data
  Star Schema   Organize analytical data
  Power BI      Visualize business insights

------------------------------------------------------------------------

# 7. Why These Technologies

## Python

**Use:** Synthetic transaction generation.

**Why:** Easy to develop, flexible, strong data-generation ecosystem and
simple Kafka integration.

Python is not the distributed processing engine. It primarily simulates
the source system.

## Apache Kafka

**Use:** Real-time event ingestion.

**Why:** Decouples the transaction source from the processing engine and
provides a durable event-streaming mechanism suitable for high-volume
continuous events.

``` text
Python -> Kafka -> PySpark
```

## PySpark Structured Streaming

**Use:** Main streaming processing engine.

**Why:** Provides structured DataFrame/SQL processing for continuous
streams, Kafka integration, transformations, window calculations and
scalable processing.

Important distinction:

``` text
Kafka = transports events
PySpark = processes events
```

Use Structured Streaming rather than the older DStream-based Spark
Streaming API.

## Delta Lake

**Use:** Bronze and Silver storage.

**Why:** Provides reliable table storage with ACID transactions, schema
enforcement, versioning/time travel and support for streaming and batch
workloads.

Important distinction:

``` text
PySpark = compute
Delta Lake = storage/table layer
```

## Apache Airflow

**Use:** Silver-to-Gold orchestration.

**Why:** Provides DAGs, dependencies, scheduling, retries, monitoring
and workflow history.

Important distinction:

``` text
PySpark = processes data
Airflow = manages tasks/workflows
```

Airflow is not the real-time streaming engine.

## SQLite

**Use:** Gold analytical storage.

**Why:** Lightweight, serverless, SQL-based and easy to deploy for a
project-scale Gold layer.

SQLite is a project-scale choice. It is not being claimed as the best
database for a massive production bank.

## Power BI

**Use:** Business intelligence.

**Why:** Provides interactive dashboards, filters, KPIs and analytical
visualizations.

------------------------------------------------------------------------

# 8. Data Generation

Use synthetic transactions only.

## Transaction Types

``` text
TRANSFER
UPI_PAYMENT
CARD_PAYMENT
ATM_WITHDRAWAL
DEPOSIT
BILL_PAYMENT
REFUND
```

## Channels

``` text
MOBILE
WEB
ATM
POS
BRANCH
UPI
```

## Status

``` text
SUCCESS
FAILED
PENDING
REVERSED
```

## Example Event

``` json
{
  "transaction_id": "TX-000001",
  "account_id": "ACC-00123",
  "transaction_type": "UPI_PAYMENT",
  "amount": 1850.50,
  "currency": "INR",
  "timestamp": "2026-08-22T10:15:20",
  "channel": "UPI",
  "status": "SUCCESS",
  "merchant_category": "RETAIL",
  "city": "Hyderabad",
  "device_id": "DEV-2031"
}
```

The generator should produce mostly normal transactions and
intentionally inject a controlled number of anomalies for
demonstration/testing.

------------------------------------------------------------------------

# 9. Canonical Transaction Schema

  Field                 Type               Required Meaning
  --------------------- ---------------- ---------- -------------------------
  transaction_id        string                  Yes Unique transaction ID
  account_id            string                  Yes Synthetic account ID
  transaction_type      string                  Yes Transaction category
  amount                decimal/double          Yes Amount
  currency              string                  Yes Currency
  timestamp             timestamp               Yes Event time
  channel               string                  Yes Source channel
  status                string                  Yes Transaction status
  merchant_category     string                   No Merchant category
  city                  string                   No Synthetic city
  device_id             string                   No Synthetic device
  source                string                  Yes Event source
  ingestion_timestamp   timestamp               Yes Pipeline ingestion time

------------------------------------------------------------------------

# 10. Kafka Design

Recommended topics:

``` text
bank_transactions
bank_transactions_quarantine
bank_transactions_anomaly
```

The primary producer is the Python generator.

The primary consumer is PySpark Structured Streaming.

Concepts the team must understand:

-   Producer
-   Consumer
-   Topic
-   Partition
-   Offset
-   Consumer group
-   Retention
-   Bootstrap server

Do not confuse Kafka with the analytical database.

------------------------------------------------------------------------

# 11. PySpark Processing Flow

``` text
Kafka
  |
  v
Read Stream
  |
  v
Parse JSON
  |
  v
Apply Schema
  |
  v
Validate
  |------ invalid ------> Quarantine
  |
 valid
  |
  v
Clean + Transform
  |
  v
Feature Engineering
  |
  v
Anomaly Monitoring
  |
  v
Bronze/Silver Delta
```

Processing responsibilities:

-   Parse Kafka values
-   Apply strict schema
-   Cast data types
-   Validate fields
-   Detect duplicates
-   Normalize categories
-   Normalize timestamps
-   Calculate derived fields
-   Calculate rolling-window features
-   Calculate anomaly score
-   Write results
-   Maintain checkpoints

------------------------------------------------------------------------

# 12. Validation Rules

Required fields:

``` text
transaction_id
account_id
transaction_type
amount
timestamp
status
```

Reject/quarantine when:

-   Required ID is null
-   Amount is invalid
-   Amount is negative
-   Timestamp cannot be parsed
-   Transaction type is unsupported
-   Status is unsupported
-   Record is malformed
-   Duplicate transaction ID is detected

Example invalid record:

``` json
{
  "transaction_id": null,
  "account_id": "ACC-10",
  "amount": -500,
  "timestamp": "INVALID"
}
```

Possible quarantine reasons:

``` text
MISSING_TRANSACTION_ID
NEGATIVE_AMOUNT
INVALID_TIMESTAMP
INVALID_TRANSACTION_TYPE
DUPLICATE_TRANSACTION
MALFORMED_JSON
```

------------------------------------------------------------------------

# 13. Quarantine Layer

The rule is simple:

> Bad data must not bring down the main pipeline.

Recommended quarantine fields:

``` text
raw_record
failure_reason
validation_timestamp
source_topic
partition
offset
```

Flow:

``` text
                 Input
                   |
                Validate
                /       \
             Valid      Invalid
               |           |
               v           v
            Bronze     Quarantine
```

Quarantined records can later be inspected or replayed after correction.

------------------------------------------------------------------------

# 14. Medallion Architecture

## Bronze

Purpose: preserve incoming valid transaction data with minimal
transformation.

Example:

``` text
transaction_id
account_id
transaction_type
amount
timestamp
status
channel
source
ingestion_timestamp
```

## Silver

Purpose: trustworthy, cleaned and enriched data.

Operations:

-   Type casting
-   Normalization
-   Deduplication
-   Enrichment
-   Feature engineering
-   Anomaly indicators
-   Standardized categories

## Gold

Purpose: business-ready analytics.

Contains:

-   Fact tables
-   Dimension tables
-   KPI tables
-   Anomaly summary tables

Mental model:

``` text
Bronze = preserve
Silver = clean
Gold = explain/measure
```

------------------------------------------------------------------------

# 15. Anomaly Monitoring

The baseline should be explainable and configurable.

Useful signals:

## High amount

Compare transaction amount against account average or rolling baseline.

## Transaction burst

Count transactions within a short window.

Example:

``` text
9 transactions in 5 minutes
```

## Failed transaction burst

Many failures within a short window.

## New device

Transaction from a previously unseen synthetic device.

## Location anomaly

Rapid location change that is unusual for the account.

## Unusual time

Activity outside the account's expected time pattern.

------------------------------------------------------------------------

# 16. Example Anomaly Score

A simple configurable score:

``` text
High amount              +2
Transaction burst        +2
New device               +1
Location anomaly         +2
Repeated failures        +2
Unusual transaction time +1
```

Example:

``` text
Score 0-2 -> NORMAL
Score 3-4 -> WATCH
Score 5+  -> ANOMALY
```

These thresholds are project parameters, not universal banking
thresholds.

Configuration should live outside code:

``` yaml
anomaly:
  high_amount_multiplier: 3.0
  burst_window_minutes: 5
  burst_transaction_limit: 8
  anomaly_score_threshold: 5
```

------------------------------------------------------------------------

# 17. Anomaly vs Fraud

This distinction is mandatory.

``` text
Anomaly != Fraud
```

A large transaction may be completely legitimate.

Correct wording:

> The system flags unusual activity for investigation.

Incorrect wording:

> The system proves that the transaction is fraudulent.

A production fraud platform would require validated labelled data,
calibrated models, false-positive/false-negative analysis, governance,
security controls and operational integration.

------------------------------------------------------------------------

# 18. Streaming Windows

Use event-time windows for useful real-time calculations.

Example 5-minute window:

``` text
transactions per account
total amount per account
failed transactions per account
unique devices per account
```

Example:

``` text
ACC001
10:00-10:05
Transactions = 12
Total = ₹48,000
Failures = 4
```

The project should define an appropriate watermark/late-data strategy.

------------------------------------------------------------------------

# 19. Event Time vs Processing Time

**Event time:** when the transaction happened.

**Processing time:** when our system processed it.

Example:

``` text
Transaction happened: 10:00
Arrived at pipeline: 10:03
```

For streaming analytics, event time should be preferred where
appropriate so calculations represent the actual transaction timeline.

------------------------------------------------------------------------

# 20. Delta Lake Layout

Recommended:

``` text
data/
├── bronze/
│   └── transactions/
├── silver/
│   └── transactions/
└── quarantine/
    └── transactions/
```

Each Delta table should have a clear schema and checkpoint location.

Do not store raw and Gold data in the same table.

------------------------------------------------------------------------

# 21. Gold Star Schema

``` text
                    dim_account
                         |
                         |
dim_date ---- fact_transactions ---- dim_transaction_type
                         |
                         |
                    dim_channel
                         |
                         |
                    dim_location
```

## Fact Table

`fact_transactions`

``` text
transaction_key
transaction_id
account_key
date_key
transaction_type_key
channel_key
location_key
amount
status
anomaly_score
is_anomaly
```

## Dimensions

### `dim_account`

``` text
account_key
account_id
account_type
customer_segment
```

### `dim_date`

``` text
date_key
full_date
day
month
quarter
year
```

### `dim_transaction_type`

``` text
transaction_type_key
transaction_type
```

### `dim_channel`

``` text
channel_key
channel
```

### `dim_location`

``` text
location_key
city
region
```

------------------------------------------------------------------------

# 22. Gold KPI Tables

Recommended:

## `daily_transaction_metrics`

``` text
date
transaction_count
total_amount
successful_count
failed_count
anomaly_count
average_amount
```

## `hourly_transaction_metrics`

``` text
hour
transaction_count
total_amount
anomaly_count
```

## `account_risk_summary`

``` text
account_id
transaction_count
total_amount
failed_count
anomaly_count
max_anomaly_score
```

------------------------------------------------------------------------

# 23. Airflow DAG

Recommended DAG:

``` text
bankflow_gold_pipeline
```

Workflow:

``` text
start
 |
 v
validate_silver
 |
 v
load_dim_date
 |
 +--> load_dim_account
 |
 +--> load_dim_transaction_type
 |
 +--> load_dim_channel
 |
 +--> load_dim_location
 |
 v
load_fact_transactions
 |
 v
calculate_kpis
 |
 v
validate_gold
 |
 v
end
```

Airflow should orchestrate downstream transformations, not process every
Kafka event.

A practical academic setup is:

``` text
Real-time:
Kafka -> PySpark -> Delta

Scheduled:
Delta -> Airflow -> SQLite -> Power BI
```

------------------------------------------------------------------------

# 24. SQLite Design

Recommended database:

``` text
data/gold/bankflow_gold.db
```

Tables:

``` text
dim_account
dim_date
dim_transaction_type
dim_channel
dim_location
fact_transactions
daily_transaction_metrics
hourly_transaction_metrics
account_risk_summary
```

SQLite should contain refined business-ready data, not the entire raw
stream.

------------------------------------------------------------------------

# 25. Power BI Dashboard

## Page 1 - Executive Overview

KPIs:

``` text
Total Transactions
Total Transaction Value
Successful Transactions
Failed Transactions
Anomalies
Anomaly Rate
```

Charts:

-   Transaction volume over time
-   Transaction value over time
-   Transactions by type
-   Transactions by channel

## Page 2 - Transaction Analytics

-   Transaction count by type
-   Amount by channel
-   Daily transaction trend
-   Average amount
-   Success/failure distribution
-   City/region distribution

## Page 3 - Anomaly Monitoring

-   Anomaly count
-   Anomaly rate
-   Anomalies over time
-   Top anomalous accounts
-   Anomaly count by type
-   Anomaly count by channel
-   High-value transactions
-   Failed-transaction bursts

Detailed table:

``` text
Transaction ID
Account ID
Amount
Timestamp
Anomaly Score
Reasons
Status
```

## Page 4 - Account Activity

-   Transactions per account
-   Total transaction value
-   Average amount
-   Failed count
-   Anomaly count
-   Last activity
-   Device count
-   Location count

------------------------------------------------------------------------

# 26. Repository Structure

``` text
bankflow-analytics/
|
├── README.md
├── PROJECT_SPECIFICATION.md
├── LICENSE
├── .gitignore
├── .env.example
├── requirements.txt
├── pyproject.toml
|
├── config/
│   ├── app.yaml
│   ├── kafka.yaml
│   ├── anomaly.yaml
│   └── schema.yaml
|
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── quarantine/
│   └── sample/
|
├── src/
│   ├── generator/
│   │   ├── transaction_generator.py
│   │   └── distributions.py
│   |
│   ├── kafka/
│   │   ├── producer.py
│   │   └── topics.py
│   |
│   ├── streaming/
│   │   ├── kafka_reader.py
│   │   ├── schema.py
│   │   ├── validation.py
│   │   ├── transformations.py
│   │   ├── anomaly_detection.py
│   │   ├── quarantine.py
│   │   ├── bronze_writer.py
│   │   └── silver_writer.py
│   |
│   ├── gold/
│   │   ├── dimensions.py
│   │   ├── facts.py
│   │   ├── metrics.py
│   │   └── sqlite_loader.py
│   |
│   └── common/
│       ├── config.py
│       ├── logging_config.py
│       └── utils.py
|
├── airflow/
│   └── dags/
│       └── bankflow_gold_pipeline.py
|
├── sql/
│   ├── create_dimensions.sql
│   ├── create_facts.sql
│   ├── create_metrics.sql
│   └── validation_queries.sql
|
├── dashboards/
│   └── powerbi/
|
├── tests/
│   ├── unit/
│   │   ├── test_generator.py
│   │   ├── test_validation.py
│   │   ├── test_anomaly_detection.py
│   │   └── test_transformations.py
│   └── integration/
│       ├── test_kafka_pipeline.py
│       ├── test_delta_pipeline.py
│       └── test_gold_pipeline.py
|
├── scripts/
│   ├── setup_local.py
│   ├── create_topics.py
│   ├── run_generator.py
│   ├── run_streaming.py
│   └── reset_data.py
|
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── anomaly_logic.md
│   ├── deployment.md
│   ├── testing.md
│   └── demo.md
|
└── docker/
    ├── docker-compose.yml
    └── README.md
```

------------------------------------------------------------------------

# 27. File Responsibilities

  File                          Responsibility
  ----------------------------- ---------------------------------
  `transaction_generator.py`    Generate synthetic transactions
  `producer.py`                 Publish events to Kafka
  `schema.py`                   Define Spark schema
  `validation.py`               Data-quality rules
  `transformations.py`          Silver transformations
  `anomaly_detection.py`        Anomaly features and score
  `quarantine.py`               Invalid-record handling
  `bronze_writer.py`            Bronze Delta output
  `silver_writer.py`            Silver Delta output
  `dimensions.py`               Gold dimensions
  `facts.py`                    Gold fact table
  `metrics.py`                  Gold KPIs
  `sqlite_loader.py`            SQLite loading
  `bankflow_gold_pipeline.py`   Airflow DAG

------------------------------------------------------------------------

# 28. Configuration

Use configuration instead of hardcoding values.

Example:

``` text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=bank_transactions
KAFKA_QUARANTINE_TOPIC=bank_transactions_quarantine

BRONZE_PATH=./data/bronze
SILVER_PATH=./data/silver
QUARANTINE_PATH=./data/quarantine
GOLD_DB_PATH=./data/gold/bankflow_gold.db

SPARK_APP_NAME=BankFlowAnalytics
```

Never commit real credentials.

Commit only:

``` text
.env.example
```

------------------------------------------------------------------------

# 29. Recommended Dependencies

Core Python dependencies:

``` text
pyspark
delta-spark
kafka-python
pandas
numpy
faker
pyyaml
python-dotenv
pytest
sqlalchemy
```

Airflow should be installed using its official installation guidance and
a compatible Python environment.

Power BI Desktop is installed separately.

Do not blindly pin versions before checking compatibility between
Spark/PySpark and Delta Lake.

------------------------------------------------------------------------

# 30. Setup Sequence

``` text
1. Clone repository
2. Create Python virtual environment
3. Install dependencies
4. Start Kafka
5. Create Kafka topics
6. Create data directories
7. Start Python generator
8. Start PySpark streaming
9. Verify Bronze
10. Verify Silver
11. Test Quarantine
12. Test anomaly injection
13. Start Airflow
14. Run Gold DAG
15. Verify SQLite
16. Connect Power BI
17. Refresh dashboard
```

Virtual environment:

``` bash
python -m venv .venv
```

Windows:

``` bash
.venv\Scripts\activate
```

Linux/macOS:

``` bash
source .venv/bin/activate
```

Install:

``` bash
pip install -r requirements.txt
```

------------------------------------------------------------------------

# 31. End-to-End Example

A transaction:

``` json
{
  "transaction_id": "TX101",
  "account_id": "ACC501",
  "transaction_type": "UPI_PAYMENT",
  "amount": 4500,
  "currency": "INR",
  "timestamp": "2026-08-22T10:10:00",
  "channel": "UPI",
  "status": "SUCCESS"
}
```

Flow:

``` text
Python
  |
  v
Kafka
  |
  v
PySpark
  |
  +-- validation
  +-- transformation
  +-- anomaly features
  |
  v
Bronze
  |
  v
Silver
  |
  v
Airflow
  |
  v
Gold SQLite
  |
  v
Power BI
```

------------------------------------------------------------------------

# 32. Example Invalid Transaction

``` json
{
  "transaction_id": null,
  "account_id": "ACC01",
  "amount": -500,
  "timestamp": "INVALID"
}
```

Expected:

``` text
Validation
   |
   +-- missing transaction ID
   +-- negative amount
   +-- invalid timestamp
   |
   v
Quarantine
```

The valid stream continues.

------------------------------------------------------------------------

# 33. Example Anomaly

Normal:

``` text
10:00 -> ₹500
10:02 -> ₹750
10:05 -> ₹1,200
```

Anomalous pattern:

``` text
10:06 -> ₹150,000
10:06 -> ₹75,000
10:07 -> ₹90,000
```

Possible reasons:

``` text
HIGH_AMOUNT
TRANSACTION_BURST
```

Output:

``` text
anomaly_score = 5
is_anomaly = true
```

This means "requires investigation", not "confirmed fraud."

------------------------------------------------------------------------

# 34. Fault-Tolerance Design

## Kafka

Provides event buffering and distributed streaming infrastructure.

## PySpark

Uses checkpointing/recovery mechanisms.

## Quarantine

Prevents bad records from breaking the main stream.

## Delta Lake

Provides transactional/reliable table storage.

## Airflow

Can retry failed downstream tasks.

Demonstrate at least:

1.  Invalid-record isolation
2.  Spark restart/recovery
3.  Airflow retry

------------------------------------------------------------------------

# 35. Testing

## Unit Tests

Test:

-   Schema validation
-   Amount validation
-   Timestamp parsing
-   Duplicate detection
-   Anomaly score
-   Transformation functions

## Integration Tests

Test:

``` text
Python -> Kafka -> PySpark
```

and:

``` text
Silver -> Airflow -> SQLite
```

## End-to-End Test

Run the complete pipeline and verify that a generated transaction
appears in the final Gold layer.

------------------------------------------------------------------------

# 36. Important Test Cases

  Test                   Expected
  ---------------------- ----------------------------
  Valid transaction      Silver
  Missing ID             Quarantine
  Negative amount        Quarantine
  Invalid status         Quarantine
  Invalid JSON           Quarantine
  Duplicate ID           Duplicate handled
  High amount            Possible anomaly
  Burst activity         Possible anomaly
  New device             Risk signal
  Spark restart          Checkpoint recovery
  Airflow task failure   Retry
  Empty Silver           Controlled workflow result

------------------------------------------------------------------------

# 37. Performance Metrics

Measure actual values rather than inventing them.

Recommended:

### Throughput

``` text
events/second
```

### Processing latency

``` text
event time -> processing time
```

### Quarantine rate

``` text
invalid records / total records
```

### Anomaly rate

``` text
anomalies / valid records
```

### Batch duration

For Spark micro-batches where applicable.

### Airflow duration

Time taken by Gold DAG.

### Gold load success

Whether all expected records reached SQLite.

------------------------------------------------------------------------

# 38. Example Evaluation Table

Use measured values during testing:

  Metric                  Measured Value
  --------------------- ----------------
  Events generated                   TBD
  Events processed                   TBD
  Valid records                      TBD
  Quarantined records                TBD
  Anomalies injected                 TBD
  Anomalies detected                 TBD
  Average latency                    TBD
  P95 latency                        TBD
  Throughput                         TBD
  Gold records                       TBD

Never replace TBD with invented numbers.

------------------------------------------------------------------------

# 39. Logging

Recommended logs:

``` text
INFO  Generated transaction TX001
INFO  Published TX001 to Kafka
INFO  Processed batch: 500
INFO  Valid records: 492
WARN  Quarantined records: 8
INFO  Anomalies detected: 5
INFO  Silver write completed
INFO  Gold load completed
```

Never log sensitive banking credentials or real PII.

------------------------------------------------------------------------

# 40. Data Lineage

Every transaction should remain traceable:

``` text
Kafka event
   |
   v
Bronze
   |
   v
Silver
   |
   v
Gold fact
   |
   v
Power BI metric
```

Preserve `transaction_id` throughout the pipeline.

Where practical, retain source topic/partition/offset metadata for
debugging.

------------------------------------------------------------------------

# 41. Git Workflow

Suggested branches:

``` text
main
develop
feature/generator
feature/kafka
feature/streaming
feature/anomaly
feature/gold
feature/airflow
feature/powerbi
```

Suggested commits:

``` text
feat: add transaction generator
feat: add kafka producer
feat: add spark streaming validation
feat: add quarantine layer
feat: add anomaly scoring
feat: add gold star schema
feat: add airflow dag
feat: add powerbi documentation
test: add validation tests
docs: add architecture documentation
```

------------------------------------------------------------------------

# 42. Team Division

For three members:

## Member 1 - Streaming

-   Python generator
-   Kafka
-   Spark ingestion
-   Schema
-   Validation
-   Quarantine

## Member 2 - Data Engineering

-   Delta Lake
-   Silver transformations
-   Anomaly features
-   Gold Star Schema
-   SQLite
-   Airflow

## Member 3 - Analytics

-   Power BI
-   KPI definitions
-   Dashboard design
-   Testing
-   Documentation
-   Demo

All members must understand the entire architecture.

------------------------------------------------------------------------

# 43. Demo Procedure

1.  Start Kafka.
2.  Create/verify `bank_transactions`.
3.  Start the Python generator.
4.  Show events entering Kafka.
5.  Start PySpark.
6.  Show Bronze output.
7.  Show Silver output.
8.  Inject malformed transaction.
9.  Show Quarantine.
10. Inject high-value/burst transaction.
11. Show anomaly score/reasons.
12. Run Airflow Gold DAG.
13. Open SQLite.
14. Show fact/dimension tables.
15. Refresh Power BI.
16. Explain dashboard metrics.

------------------------------------------------------------------------

# 44. What the Business Actually Uses

The business does not directly use Kafka or PySpark.

The technical infrastructure runs the pipeline:

``` text
Customer activity
      |
      v
Data pipeline
      |
      v
Processed analytics
      |
      v
Power BI
```

Business users mainly interact with Power BI dashboards.

Technical/data engineering teams manage Kafka, Spark, Delta, Airflow and
SQLite.

The project is therefore a **data platform**, not a customer-facing
banking application.

------------------------------------------------------------------------

# 45. Production Upgrade Path

The academic implementation can later be upgraded:

``` text
SQLite
   -> PostgreSQL / Warehouse / Lakehouse serving layer

Local Kafka
   -> Managed Kafka

Local Spark
   -> Distributed Spark

Rule-based anomaly
   -> Calibrated ML/streaming model

Local Power BI
   -> Power BI Service

Local monitoring
   -> Prometheus/Grafana/logging platform
```

Production security would additionally require authentication,
authorization, encryption, secret management, audit logging and
appropriate governance.

------------------------------------------------------------------------

# 46. Future Scope

1.  Add Isolation Forest.
2.  Add autoencoder-based anomaly detection.
3.  Compare rules vs ML.
4.  Evaluate precision, recall, F1 and PR-AUC when labelled data is
    available.
5.  Add explainability.
6.  Add model monitoring.
7.  Add drift detection.
8.  Deploy to cloud.
9.  Add centralized monitoring.
10. Add alert delivery.
11. Add production database/warehouse.
12. Add real-time serving if required.

------------------------------------------------------------------------

# 47. Research Potential

A possible research direction is:

> **Explainable Real-Time Anomaly Monitoring for Streaming Banking
> Transactions Using Distributed Data Processing**

Potential research questions:

-   How does latency change as transaction throughput increases?
-   How effective are transaction-frequency features?
-   How do rule-based and unsupervised methods compare?
-   What is the trade-off between anomaly sensitivity and false
    positives?
-   How does Spark configuration affect streaming performance?

Combining common technologies is not by itself a research contribution.
The paper needs a measurable research question, methodology, experiments
and results.

------------------------------------------------------------------------

# 48. Project Milestones

## Milestone 1

Architecture, schema and repository.

## Milestone 2

Python transaction generator.

## Milestone 3

Kafka producer/topic.

## Milestone 4

PySpark Kafka consumer.

## Milestone 5

Validation + Quarantine.

## Milestone 6

Bronze/Silver Delta.

## Milestone 7

Anomaly monitoring.

## Milestone 8

Gold Star Schema + SQLite.

## Milestone 9

Airflow DAG.

## Milestone 10

Power BI.

## Milestone 11

Testing and performance measurement.

## Milestone 12

Documentation, presentation and final demo.

------------------------------------------------------------------------

# 49. Minimum Viable Project

The MVP is complete when all of these work:

``` text
[ ] Python generates transactions
[ ] Kafka receives transactions
[ ] PySpark consumes Kafka
[ ] Schema validation works
[ ] Invalid records are quarantined
[ ] Bronze Delta exists
[ ] Silver Delta exists
[ ] Anomaly score is calculated
[ ] Airflow DAG runs
[ ] SQLite Gold database is populated
[ ] Star Schema exists
[ ] Power BI dashboard works
[ ] End-to-end demo works
```

------------------------------------------------------------------------

# 50. Definition of Done

The project is complete only when:

1.  The pipeline runs end-to-end.
2.  Valid records reach Silver.
3.  Invalid records reach Quarantine.
4.  Bronze preserves incoming data.
5.  Silver is cleaned and typed.
6.  Anomalies can be reproducibly generated.
7.  Anomalies are detected with explainable reasons.
8.  Gold tables are correctly populated.
9.  Star Schema relationships are valid.
10. Airflow successfully orchestrates Gold processing.
11. Power BI displays correct values.
12. Failure/restart scenarios are tested.
13. Documentation is sufficient for a new developer.
14. No real sensitive banking data is used.
15. Performance values are measured rather than invented.

------------------------------------------------------------------------

# 51. Common Mistakes to Avoid

-   Do not call SQLite a big-data database.
-   Do not call Airflow a streaming engine.
-   Do not call Kafka an analytical database.
-   Do not call every anomaly fraud.
-   Do not claim "instant" dashboards without measuring latency.
-   Do not invent performance metrics.
-   Do not use real banking information.
-   Do not start with ML before the streaming pipeline works.
-   Do not hardcode secrets.
-   Do not claim a feature is implemented if it is only planned.

------------------------------------------------------------------------

# 52. Viva Cheat Sheet

### What is the project?

A real-time banking transaction analytics and anomaly monitoring data
pipeline.

### Is it a website?

No. It is primarily a backend/data engineering platform with Power BI as
the business-facing visualization layer.

### Why Kafka?

Continuous event streaming, buffering and decoupling.

### Why PySpark?

Structured distributed stream processing.

### Why Delta Lake?

Reliable Bronze/Silver storage with transactional and schema-management
capabilities.

### Why Airflow?

Workflow orchestration, scheduling, dependencies, retries and
monitoring.

### Why SQLite?

Lightweight SQL storage for the project-scale Gold layer.

### Why Power BI?

Business intelligence dashboards.

### What is Medallion Architecture?

``` text
Bronze -> initial
Silver -> cleaned
Gold -> business-ready
```

### What is a Quarantine Layer?

A place for invalid/malformed records so they do not interrupt the main
pipeline.

### What is Star Schema?

A central fact table connected to descriptive dimension tables.

### What is ACID?

Atomicity, Consistency, Isolation and Durability.

### Does anomaly mean fraud?

No. It means unusual activity that may require investigation.

### Is Airflow real-time?

No. Kafka + PySpark form the main real-time path. Airflow orchestrates
downstream workflows.

### How does the entire pipeline work?

> Python generates transactions, Kafka streams them, PySpark validates
> and processes them, Delta Lake stores Bronze/Silver data, Airflow
> orchestrates Gold transformation, SQLite stores the Gold Star Schema,
> and Power BI visualizes the resulting business insights.

------------------------------------------------------------------------

# 53. Official Technical References

Use official documentation when implementing and pinning versions:

-   Apache Kafka: https://kafka.apache.org/documentation/
-   Apache Spark Structured Streaming:
    https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
-   Delta Lake: https://docs.delta.io/
-   Apache Airflow: https://airflow.apache.org/docs/
-   SQLite: https://sqlite.org/docs.html
-   Microsoft Power BI: https://learn.microsoft.com/power-bi/

Always verify compatibility between Spark/PySpark and Delta Lake before
fixing dependency versions.

------------------------------------------------------------------------

# 54. Canonical AI Context

If this document is given to another AI, treat the following as the
authoritative project definition:

``` text
PROJECT NAME:
BankFlow Analytics

FULL TITLE:
Real-Time Banking Transaction Analytics & Anomaly Monitoring Platform

DOMAIN:
Banking transaction analytics and anomaly monitoring.

TYPE:
Academic end-to-end real-time data engineering and BI platform.

CANONICAL PIPELINE:
Python -> Kafka -> PySpark Structured Streaming -> Delta Lake -> Airflow -> SQLite -> Power BI

DATA LAYERS:
Bronze -> Silver -> Gold

BRONZE:
Preserved valid incoming transaction data.

SILVER:
Validated, cleaned, transformed and enriched data.

GOLD:
Business-ready Star Schema and KPI data.

QUARANTINE:
Malformed/invalid records plus validation reasons.

PYTHON:
Synthetic event generator.

KAFKA:
Real-time event streaming and buffering.

PYSPARK:
Streaming processing, validation, transformation, feature calculation and anomaly monitoring.

DELTA LAKE:
Bronze/Silver storage layer.

AIRFLOW:
Silver-to-Gold workflow orchestration.

SQLITE:
Gold analytical database.

STAR SCHEMA:
fact_transactions plus account/date/type/channel/location dimensions.

POWER BI:
Business intelligence and visualization.

ANOMALY:
Unusual activity according to configured rules/features. It is not proof of fraud.

DATA:
Synthetic only.

PRIMARY BUSINESS OUTPUTS:
Transaction volume, transaction value, success/failure, account activity, anomaly count, anomaly rate and anomaly reasons.

IMPORTANT:
Do not invent completed features, performance results or integrations. Clearly label future features as planned.

IMPORTANT:
Do not use real customer banking data.

IMPORTANT:
Do not describe SQLite as enterprise-scale banking storage.

IMPORTANT:
Do not describe Airflow as the real-time stream processor.

IMPORTANT:
Do not describe anomaly detection as guaranteed fraud detection.

IMPLEMENTATION ORDER:
Generator -> Kafka -> PySpark -> Validation -> Quarantine -> Bronze -> Silver -> Anomaly -> Airflow -> Gold -> SQLite -> Power BI -> Testing -> Documentation.
```

------------------------------------------------------------------------

# 55. Final Mental Model

``` text
BANKING TRANSACTION
        |
        v
      PYTHON
  "create event"
        |
        v
      KAFKA
  "stream event"
        |
        v
     PYSPARK
 "process + validate"
        |
      /   \
 valid    invalid
  |          |
  v          v
BRONZE    QUARANTINE
  |
  v
SILVER
  |
  v
AIRFLOW
"orchestrate"
  |
  v
SQLITE GOLD
"Star Schema"
  |
  v
POWER BI
"business insight"
```

**The core idea is:**

> **Generate -\> Stream -\> Process -\> Validate -\> Store -\>
> Orchestrate -\> Analyze -\> Visualize.**
