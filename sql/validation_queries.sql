-- ---------------------------------------------------------------------------
-- BankFlow Analytics - Gold Layer: Data quality / validation queries
-- Run these after each Silver-to-Gold load to sanity-check the warehouse.
-- ---------------------------------------------------------------------------

-- 1. Orphaned fact rows (dimension keys that don't resolve)
SELECT COUNT(*) AS orphaned_account_fk
FROM fact_transactions f
LEFT JOIN dim_account a ON f.account_key = a.account_key
WHERE a.account_key IS NULL;

SELECT COUNT(*) AS orphaned_date_fk
FROM fact_transactions f
LEFT JOIN dim_date d ON f.date_key = d.date_key
WHERE d.date_key IS NULL;

-- 2. Duplicate transaction_ids in the fact table (should always be 0)
SELECT transaction_id, COUNT(*) AS cnt
FROM fact_transactions
GROUP BY transaction_id
HAVING cnt > 1;

-- 3. Negative or zero amounts that slipped through (should always be 0 rows)
SELECT COUNT(*) AS invalid_amounts
FROM fact_transactions
WHERE amount <= 0;

-- 4. Anomaly flag / score consistency (is_anomaly=1 must have score >= 5)
SELECT COUNT(*) AS inconsistent_anomaly_flags
FROM fact_transactions
WHERE (is_anomaly = 1 AND anomaly_score < 5)
   OR (is_anomaly = 0 AND anomaly_score >= 5);

-- 5. Row counts per layer for a quick health check
SELECT 'fact_transactions' AS table_name, COUNT(*) AS row_count FROM fact_transactions
UNION ALL
SELECT 'dim_account', COUNT(*) FROM dim_account
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'daily_transaction_metrics', COUNT(*) FROM daily_transaction_metrics
UNION ALL
SELECT 'account_risk_summary', COUNT(*) FROM account_risk_summary;

-- 6. Latest loaded event timestamp (freshness check)
SELECT MAX(event_timestamp) AS latest_event_timestamp FROM fact_transactions;

-- 7. Daily metrics reconciliation against fact table
SELECT
    dtm.date_key,
    dtm.total_transactions AS metric_total,
    (SELECT COUNT(*) FROM fact_transactions ft WHERE ft.date_key = dtm.date_key) AS fact_total
FROM daily_transaction_metrics dtm
WHERE dtm.total_transactions <> (
    SELECT COUNT(*) FROM fact_transactions ft WHERE ft.date_key = dtm.date_key
);
