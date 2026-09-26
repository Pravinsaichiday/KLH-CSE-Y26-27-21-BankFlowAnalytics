"""
Statistical distributions used to synthesize realistic banking transaction
data: transaction amounts follow a log-normal/Pareto-like distribution
(most transactions small, a long tail of large ones), and timestamps have
a diurnal (time-of-day) weighting so activity looks human.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import List

import numpy as np

# Base per-account rolling "typical" amount, used later by anomaly injection
# to synthesize amounts that are `> multiplier * mean`.
_DEFAULT_MEAN_LOG = 6.5   # ln(~665)
_DEFAULT_SIGMA_LOG = 0.9  # spread


def sample_transaction_amount(
    rng: random.Random,
    mean_log: float = _DEFAULT_MEAN_LOG,
    sigma_log: float = _DEFAULT_SIGMA_LOG,
) -> float:
    """
    Sample a transaction amount from a log-normal distribution, which
    approximates the heavy-tailed nature of real transaction values
    (many small purchases, occasional large transfers).
    """
    np_rng = np.random.default_rng(rng.randint(0, 2**32 - 1))
    value = np_rng.lognormal(mean=mean_log, sigma=sigma_log)
    return round(max(value, 1.0), 2)


def sample_pareto_tail(rng: random.Random, scale: float = 5000.0, shape: float = 2.5) -> float:
    """Sample from a Pareto distribution — used for rare large-value events."""
    np_rng = np.random.default_rng(rng.randint(0, 2**32 - 1))
    value = (np_rng.pareto(shape) + 1) * scale
    return round(value, 2)


# Diurnal weighting: index 0-23 -> relative likelihood of a transaction
# occurring in that hour. Peaks around 10am-1pm and 6pm-9pm; troughs at night.
HOURLY_WEIGHTS: List[float] = [
    0.2, 0.15, 0.1, 0.1, 0.15, 0.3,   # 00-05 (low activity / "unusual hours")
    0.6, 1.0, 1.4, 1.6, 1.8, 1.9,     # 06-11
    2.0, 1.8, 1.6, 1.5, 1.6, 1.8,     # 12-17
    2.1, 2.2, 1.9, 1.4, 0.9, 0.4,     # 18-23
]


def sample_event_hour(rng: random.Random) -> int:
    """Pick an hour-of-day weighted by realistic diurnal transaction volume."""
    total = sum(HOURLY_WEIGHTS)
    r = rng.uniform(0, total)
    cumulative = 0.0
    for hour, weight in enumerate(HOURLY_WEIGHTS):
        cumulative += weight
        if r <= cumulative:
            return hour
    return 23


def jitter_timestamp(base: datetime, max_seconds: int = 3) -> datetime:
    """Add small random jitter to a timestamp to avoid perfectly uniform spacing."""
    delta = timedelta(seconds=random.uniform(-max_seconds, max_seconds))
    return base + delta


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
