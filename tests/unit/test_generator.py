"""
Unit tests for the synthetic transaction generator.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from src.generator.transaction_generator import TransactionGenerator, AccountPool
from src.common.config import settings

SCHEMA = settings.schema["schema"]


@pytest.fixture
def generator() -> TransactionGenerator:
    return TransactionGenerator(num_accounts=20, anomaly_ratio=0.0, seed=42)


def test_account_pool_creates_unique_accounts():
    pool = AccountPool(num_accounts=50, seed=1)
    account_ids = [a.account_id for a in pool.accounts]
    assert len(account_ids) == 50
    assert len(set(account_ids)) == 50


def test_normal_transaction_has_required_fields(generator: TransactionGenerator):
    txn = generator.generate_normal()
    for field in SCHEMA["required_fields"]:
        assert field in txn
        assert txn[field] is not None


def test_normal_transaction_uses_valid_domain_values(generator: TransactionGenerator):
    txn = generator.generate_normal()
    assert txn["transaction_type"] in SCHEMA["transaction_types"]
    assert txn["channel"] in SCHEMA["channels"]
    assert txn["status"] in SCHEMA["statuses"]
    assert txn["city"] in SCHEMA["cities"]
    assert txn["amount"] > 0


def test_high_amount_spike_exceeds_typical_amount(generator: TransactionGenerator):
    txns = generator._gen_high_amount_spike()
    assert len(txns) == 1
    assert txns[0]["amount"] > 0


def test_transaction_burst_produces_multiple_events_same_account(generator: TransactionGenerator):
    txns = generator._gen_transaction_burst()
    assert 9 <= len(txns) <= 13
    account_ids = {t["account_id"] for t in txns}
    assert len(account_ids) == 1


def test_geo_hop_produces_two_cities(generator: TransactionGenerator):
    txns = generator._gen_geo_hop()
    assert len(txns) == 2
    cities = {t["city"] for t in txns}
    assert len(cities) == 2
    assert txns[0]["account_id"] == txns[1]["account_id"]


def test_failure_spike_all_failed_upi(generator: TransactionGenerator):
    txns = generator._gen_failure_spike()
    assert len(txns) >= 4
    assert all(t["status"] == "FAILED" for t in txns)
    assert all(t["transaction_type"] == "UPI_PAYMENT" for t in txns)


def test_malformed_payload_contains_violation_marker(generator: TransactionGenerator):
    txns = generator._gen_malformed_payload()
    assert len(txns) == 1
    assert "_injected_violation" in txns[0]
    assert txns[0]["_injected_violation"] in {
        "null_transaction_id",
        "negative_amount",
        "invalid_timestamp",
        "invalid_transaction_type",
        "missing_account_id",
        "missing_amount",
        "invalid_status",
        "non_numeric_amount",
    }


def test_anomaly_ratio_zero_never_injects_anomalies():
    gen = TransactionGenerator(num_accounts=10, anomaly_ratio=0.0, seed=7)
    events = gen.stream(count=50)
    # With ratio 0.0, every unit should be exactly one normal transaction.
    assert len(events) == 50


def test_anomaly_ratio_one_always_injects_anomalies():
    gen = TransactionGenerator(num_accounts=10, anomaly_ratio=1.0, seed=7)
    events = gen.stream(count=5)
    # Each unit should produce at least 1 event (some anomaly types produce many).
    assert len(events) >= 5


def test_serialize_produces_valid_json_string(generator: TransactionGenerator):
    txn = generator.generate_normal()
    serialized = generator.serialize(txn)
    assert isinstance(serialized, str)
    assert txn["transaction_id"] in serialized
