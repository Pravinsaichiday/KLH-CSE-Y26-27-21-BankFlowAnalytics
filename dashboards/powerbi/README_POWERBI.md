# BankFlow Analytics — Power BI Dashboard Specification

This document specifies how to connect Power BI to the Gold layer
(`data/gold/bankflow_gold.db`, a SQLite star schema) and how to build the
four analytical dashboard pages, including all required DAX measures.

---

## 1. Data Source Connection

SQLite has no native Power BI connector, so connect via **ODBC**:

1. Install a SQLite ODBC driver (e.g. the "SQLite3 ODBC Driver" from
   ch-werner.de, or the `sqliteodbc` package).
2. Create a **System DSN** (ODBC Data Source Administrator) pointing at
   `data/gold/bankflow_gold.db`.
3. In Power BI Desktop: **Get Data → ODBC** → select the DSN.
4. Power BI will only support **Import** mode reliably through the ODBC
   driver (DirectQuery over generic ODBC is limited); for a pipeline that
   refreshes every 15 minutes via Airflow, **Import with scheduled
   refresh** is the recommended mode. If low-latency DirectQuery is a hard
   requirement, consider periodically materializing the Gold tables into a
   server-based engine (e.g. Postgres) that Power BI supports natively.
5. Import these tables: `dim_account`, `dim_date`, `dim_transaction_type`,
   `dim_channel`, `dim_location`, `fact_transactions`,
   `daily_transaction_metrics`, `hourly_transaction_metrics`,
   `account_risk_summary`.

### 1.1 Model Relationships (Star Schema)

Create the following **1-to-many** relationships, all filtering from the
"1" side (dimension) to the "many" side (`fact_transactions`):

| From (1)                  | To (many)                              | Key                     |
|----------------------------|-----------------------------------------|--------------------------|
| `dim_account[account_key]` | `fact_transactions[account_key]`        | Single direction        |
| `dim_date[date_key]`       | `fact_transactions[date_key]`           | Single direction        |
| `dim_transaction_type[transaction_type_key]` | `fact_transactions[transaction_type_key]` | Single direction |
| `dim_channel[channel_key]` | `fact_transactions[channel_key]`        | Single direction        |
| `dim_location[location_key]` | `fact_transactions[location_key]`     | Single direction        |
| `dim_date[date_key]`       | `daily_transaction_metrics[date_key]`   | Single direction        |
| `dim_date[date_key]`       | `hourly_transaction_metrics[date_key]`  | Single direction        |
| `dim_account[account_key]` | `account_risk_summary[account_key]`     | Single direction        |

Mark `dim_date` as a **Date Table** (Modeling → Mark as Date Table) using
`dim_date[full_date]`.

---

## 2. DAX Measures

Create these in a dedicated measures table (e.g. `_Measures`):

```dax
Total Transactions = COUNTROWS(fact_transactions)

Total Volume = SUM(fact_transactions[amount])

Success Rate =
DIVIDE(
    CALCULATE(COUNTROWS(fact_transactions), fact_transactions[status] = "SUCCESS"),
    [Total Transactions],
    0
)

Anomaly Count =
CALCULATE(COUNTROWS(fact_transactions), fact_transactions[is_anomaly] = TRUE())

Anomaly Rate = DIVIDE([Anomaly Count], [Total Transactions], 0)

Avg Anomaly Score = AVERAGE(fact_transactions[anomaly_score])
```

### 2.1 Supporting measures (recommended additions)

```dax
Failed Transactions =
CALCULATE(COUNTROWS(fact_transactions), fact_transactions[status] = "FAILED")

Failure Rate = DIVIDE([Failed Transactions], [Total Transactions], 0)

Avg Transaction Value = AVERAGE(fact_transactions[amount])

Watch-Band Accounts =
CALCULATE(
    DISTINCTCOUNT(account_risk_summary[account_key]),
    account_risk_summary[risk_band] = "WATCH"
)

High-Risk Accounts =
CALCULATE(
    DISTINCTCOUNT(account_risk_summary[account_key]),
    account_risk_summary[risk_band] = "ANOMALY"
)

Volume MoM % =
VAR CurrentVolume = [Total Volume]
VAR PriorVolume = CALCULATE([Total Volume], DATEADD(dim_date[full_date], -1, MONTH))
RETURN DIVIDE(CurrentVolume - PriorVolume, PriorVolume, 0)
```

> **Note on anomaly semantics:** `is_anomaly` and `anomaly_score` represent
> *unusual activity flagged for investigation*, not confirmed fraud. Any
> dashboard labels or tooltips referencing these fields should say
> "flagged" / "under review" rather than "fraudulent".

---

## 3. Dashboard Pages

### Page 1 — Executive Overview
- **KPI Cards** (top row): `Total Transactions`, `Total Volume`,
  `Success Rate`, `Anomaly Rate`.
- **Volume & Value Trend**: Line chart, X = `dim_date[full_date]`,
  Y = `Total Volume` and `Total Transactions` (dual axis), sourced from
  `daily_transaction_metrics` for fast rendering.
- **Breakdown by Transaction Type**: Donut/bar chart,
  `dim_transaction_type[transaction_type]` vs `[Total Volume]`.
- **Breakdown by Channel**: Bar chart, `dim_channel[channel]` vs
  `[Total Transactions]`.
- **Slicers**: Date range, transaction type, channel.

### Page 2 — Transaction Analytics
- **Distribution by City**: Map or bar chart, `dim_location[city]` vs
  `[Total Volume]` / `[Total Transactions]`.
- **Average Transaction Size**: Card + trend line using
  `[Avg Transaction Value]`, sliceable by transaction type and channel.
- **Success vs Failure**: Stacked column chart, X = `dim_date[full_date]`,
  Y = success count and failed count (from `daily_transaction_metrics`).
- **Hourly Activity Heatmap**: Matrix visual, rows = `hour_of_day`
  (from `hourly_transaction_metrics`), columns = day of week, values =
  `total_transactions`.

### Page 3 — Anomaly Monitoring
- **Scatter Plot — Amount vs Anomaly Score**: X = `amount`,
  Y = `anomaly_score`, sized by count, colored by `anomaly_band`
  (derive a calculated column/measure bucketing score into
  NORMAL/WATCH/ANOMALY per the `config/anomaly.yaml` bands).
- **Top High-Risk Accounts**: Table/bar chart from `account_risk_summary`,
  sorted by `max_anomaly_score` descending, columns: `account_id`,
  `total_transactions`, `anomaly_count`, `max_anomaly_score`, `risk_band`.
- **Drill-down Table with Reason Breakdown**: Table visual on
  `fact_transactions` filtered to `is_anomaly = TRUE`, columns:
  `transaction_id`, `account_id`, `amount`, `anomaly_score`,
  `anomaly_reasons`, `event_timestamp`. Use a tooltip page to expand
  `anomaly_reasons` (comma-delimited) into a readable list.
- **Anomaly Trend**: Line chart of `[Anomaly Count]` and `[Anomaly Rate]`
  over `dim_date[full_date]`.

### Page 4 — Account Activity & Profiling
- **Customer Segment Behavior**: Bar/column chart,
  `dim_account[customer_segment]` vs `[Total Volume]` and
  `[Total Transactions]`.
- **Burst Detection**: Table/visual highlighting accounts where
  `fact_transactions[anomaly_reasons]` contains `"TRANSACTION_BURST"`
  (use a calculated column with `SEARCH()` or pre-filter in the SQL
  extraction layer).
- **Device Diversity**: Bar chart of distinct `device_id` count per
  account (bucketed), sourced from Silver-layer window features
  (`window_distinct_devices`) if exposed into Gold, or approximated via
  a supplementary extract.
- **Account Risk Distribution**: Donut chart of `account_risk_summary`
  grouped by `risk_band` (NORMAL / WATCH / ANOMALY).

---

## 4. Refresh Cadence

Since Airflow refreshes the Gold SQLite database every 15 minutes
(`airflow/dags/bankflow_gold_pipeline.py`), configure Power BI Desktop's
scheduled refresh (via the on-premises data gateway, since SQLite is a
local file) to match — a 15–30 minute cadence keeps the dashboard close to
real time without over-polling the file.
