"""
Synthetic banking transaction generator for BankFlow Analytics.

Generates fully synthetic (no real PII) transaction events matching the
canonical schema, with a configurable anomaly-injection engine capable of
producing:
    a) sudden high-amount spikes
    b) rapid transaction bursts for a single account
    c) fast geographical hops (impossible travel)
    d) rapid consecutive failure spikes
    e) deliberate schema violations / malformed payloads (for quarantine testing)

All PII-shaped fields (names, etc.) are generated via Faker purely to make
account/customer records look structurally realistic; nothing here
represents a real person or real bank account.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, List, Optional

from faker import Faker

from src.common.config import settings
from src.common.logging_config import get_logger
from src.common.utils import new_uuid, to_iso, safe_json_dumps
from src.generator.distributions import (
    sample_transaction_amount,
    sample_pareto_tail,
    sample_event_hour,
    now_utc,
)

logger = get_logger(__name__)

SCHEMA = settings.schema["schema"]
TRANSACTION_TYPES = SCHEMA["transaction_types"]
CHANNELS = SCHEMA["channels"]
STATUSES = SCHEMA["statuses"]
MERCHANT_CATEGORIES = SCHEMA["merchant_categories"]
CITIES = SCHEMA["cities"]
CURRENCY = SCHEMA["currency"]
ACCOUNT_TYPES = SCHEMA["account_types"]
CUSTOMER_SEGMENTS = SCHEMA["customer_segments"]


@dataclass
class SyntheticAccount:
    account_id: str
    account_type: str
    customer_segment: str
    home_city: str
    typical_mean_log: float
    devices: List[str] = field(default_factory=list)


class AccountPool:
    """Generates and holds a pool of synthetic (fake) accounts."""

    def __init__(self, num_accounts: int = 500, seed: Optional[int] = None):
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)
        self.rng = random.Random(seed)
        self.accounts: List[SyntheticAccount] = [
            self._make_account() for _ in range(num_accounts)
        ]

    def _make_account(self) -> SyntheticAccount:
        account_id = f"ACC{self.faker.unique.random_number(digits=10, fix_len=True)}"
        num_devices = self.rng.randint(1, 3)
        devices = [f"DEV-{new_uuid()[:8]}" for _ in range(num_devices)]
        return SyntheticAccount(
            account_id=account_id,
            account_type=self.rng.choice(ACCOUNT_TYPES),
            customer_segment=self.rng.choice(CUSTOMER_SEGMENTS),
            home_city=self.rng.choice(CITIES),
            typical_mean_log=self.rng.uniform(5.5, 7.5),
            devices=devices,
        )

    def random_account(self) -> SyntheticAccount:
        return self.rng.choice(self.accounts)


class TransactionGenerator:
    """
    Produces a stream of synthetic transaction event dicts, optionally
    injecting anomalous patterns at a configurable ratio.
    """

    def __init__(
        self,
        num_accounts: int = 500,
        anomaly_ratio: float = 0.08,
        seed: Optional[int] = None,
    ):
        self.rng = random.Random(seed)
        self.anomaly_ratio = anomaly_ratio
        self.account_pool = AccountPool(num_accounts=num_accounts, seed=seed)
        self._anomaly_generators = [
            self._gen_high_amount_spike,
            self._gen_transaction_burst,
            self._gen_geo_hop,
            self._gen_failure_spike,
            self._gen_malformed_payload,
        ]

    # ------------------------------------------------------------------
    # Normal transaction generation
    # ------------------------------------------------------------------
    def _base_transaction(
        self,
        account: SyntheticAccount,
        amount: Optional[float] = None,
        status: Optional[str] = None,
        city: Optional[str] = None,
        device_id: Optional[str] = None,
        timestamp: Optional[str] = None,
    ) -> Dict[str, Any]:
        txn_type = self.rng.choice(TRANSACTION_TYPES)
        chosen_amount = amount if amount is not None else sample_transaction_amount(
            self.rng, mean_log=account.typical_mean_log
        )
        chosen_status = status or self.rng.choices(
            STATUSES, weights=[0.88, 0.07, 0.03, 0.02], k=1
        )[0]
        event_time = now_utc() if timestamp is None else timestamp

        return {
            "transaction_id": f"TXN-{new_uuid()}",
            "account_id": account.account_id,
            "transaction_type": txn_type,
            "amount": chosen_amount,
            "currency": CURRENCY,
            "timestamp": to_iso(event_time) if not isinstance(event_time, str) else event_time,
            "channel": self.rng.choice(CHANNELS),
            "status": chosen_status,
            "merchant_category": self.rng.choice(MERCHANT_CATEGORIES),
            "city": city or account.home_city,
            "device_id": device_id or self.rng.choice(account.devices),
            "source": "synthetic-generator",
        }

    def generate_normal(self) -> Dict[str, Any]:
        account = self.account_pool.random_account()
        return self._base_transaction(account)

    # ------------------------------------------------------------------
    # Anomaly injection engine
    # ------------------------------------------------------------------
    def _gen_high_amount_spike(self) -> List[Dict[str, Any]]:
        """(a) Sudden high amount spike: > 3x the account's typical mean."""
        account = self.account_pool.random_account()
        typical_amount = sample_transaction_amount(self.rng, mean_log=account.typical_mean_log)
        spike_amount = max(typical_amount * self.rng.uniform(3.2, 8.0), sample_pareto_tail(self.rng))
        txn = self._base_transaction(account, amount=round(spike_amount, 2), status="SUCCESS")
        logger.debug("Injected high-amount spike for %s: %.2f", account.account_id, spike_amount)
        return [txn]

    def _gen_transaction_burst(self) -> List[Dict[str, Any]]:
        """(b) Rapid transaction burst: ~9 transactions in 2 minutes for one account."""
        account = self.account_pool.random_account()
        burst_size = self.rng.randint(9, 13)
        base_time = now_utc()
        txns = []
        for i in range(burst_size):
            offset = timedelta(seconds=self.rng.uniform(0, 120))
            ts = base_time + offset
            txns.append(self._base_transaction(account, timestamp=ts))
        logger.debug("Injected transaction burst of %d for %s", burst_size, account.account_id)
        return txns

    def _gen_geo_hop(self) -> List[Dict[str, Any]]:
        """(c) Fast geographical hop: e.g., Mumbai -> Delhi within 3 minutes."""
        account = self.account_pool.random_account()
        city_a, city_b = self.rng.sample(CITIES, 2)
        t1 = now_utc()
        t2 = t1 + timedelta(minutes=self.rng.uniform(1, 3))
        txn1 = self._base_transaction(account, city=city_a, timestamp=t1)
        txn2 = self._base_transaction(account, city=city_b, timestamp=t2)
        logger.debug(
            "Injected geo hop for %s: %s -> %s", account.account_id, city_a, city_b
        )
        return [txn1, txn2]

    def _gen_failure_spike(self) -> List[Dict[str, Any]]:
        """(d) Rapid consecutive failures: e.g., 4 consecutive failed UPI attempts."""
        account = self.account_pool.random_account()
        num_failures = self.rng.randint(4, 6)
        base_time = now_utc()
        txns = []
        for i in range(num_failures):
            ts = base_time + timedelta(seconds=i * self.rng.uniform(5, 20))
            txn = self._base_transaction(account, status="FAILED", timestamp=ts)
            txn["transaction_type"] = "UPI_PAYMENT"
            txns.append(txn)
        logger.debug("Injected failure spike of %d for %s", num_failures, account.account_id)
        return txns

    def _gen_malformed_payload(self) -> List[Dict[str, Any]]:
        """
        (e) Deliberate schema violations / malformed payloads, for exercising
        the quarantine / dead-letter path. Produces exactly one bad record.
        """
        account = self.account_pool.random_account()
        base = self._base_transaction(account)
        violation_type = self.rng.choice(
            [
                "null_transaction_id",
                "negative_amount",
                "invalid_timestamp",
                "invalid_transaction_type",
                "missing_account_id",
                "missing_amount",
                "invalid_status",
                "non_numeric_amount",
            ]
        )

        if violation_type == "null_transaction_id":
            base["transaction_id"] = None
        elif violation_type == "negative_amount":
            base["amount"] = -abs(base["amount"])
        elif violation_type == "invalid_timestamp":
            base["timestamp"] = "not-a-real-timestamp"
        elif violation_type == "invalid_transaction_type":
            base["transaction_type"] = "TELEPORT_PAYMENT"
        elif violation_type == "missing_account_id":
            del base["account_id"]
        elif violation_type == "missing_amount":
            del base["amount"]
        elif violation_type == "invalid_status":
            base["status"] = "UNKNOWN_STATE"
        elif violation_type == "non_numeric_amount":
            base["amount"] = "".join(self.rng.choices(string.ascii_letters, k=6))

        base["_injected_violation"] = violation_type
        logger.debug("Injected malformed payload (%s) for %s", violation_type, account.account_id)
        return [base]

    def generate_anomaly_batch(self) -> List[Dict[str, Any]]:
        """Pick one anomaly pattern at random and generate its event(s)."""
        generator_fn = self.rng.choice(self._anomaly_generators)
        return generator_fn()

    # ------------------------------------------------------------------
    # Public stream interface
    # ------------------------------------------------------------------
    def next_batch(self) -> List[Dict[str, Any]]:
        """
        Produce the next unit of work: either a single normal transaction,
        or (at `anomaly_ratio` probability) a batch of events representing
        an injected anomalous pattern.
        """
        if self.rng.random() < self.anomaly_ratio:
            return self.generate_anomaly_batch()
        return [self.generate_normal()]

    def stream(self, count: int) -> List[Dict[str, Any]]:
        """Generate `count` top-level units (normal txns or anomaly batches), flattened."""
        events: List[Dict[str, Any]] = []
        for _ in range(count):
            events.extend(self.next_batch())
        return events

    @staticmethod
    def serialize(event: Dict[str, Any]) -> str:
        return safe_json_dumps(event)
