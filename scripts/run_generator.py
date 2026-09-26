#!/usr/bin/env python3
"""
CLI: Run the synthetic transaction generator + Kafka producer loop.

Usage:
    python scripts/run_generator.py --rate 10 --anomaly-ratio 0.1 --duration 300
    python scripts/run_generator.py --rate 5 --anomaly-ratio 0.08 --duration 0   # run forever
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.common.logging_config import get_logger  # noqa: E402
from src.common.config import settings  # noqa: E402
from src.kafka.producer import run_producer_loop  # noqa: E402

logger = get_logger(__name__)


def main() -> int:
    gen_cfg = settings.app["generator"]

    parser = argparse.ArgumentParser(description="Run the BankFlow synthetic transaction generator.")
    parser.add_argument(
        "--rate", type=float, default=gen_cfg["rate_per_second"],
        help="Target events (normal txn or anomaly batch) produced per second.",
    )
    parser.add_argument(
        "--anomaly-ratio", type=float, default=gen_cfg["anomaly_ratio"],
        help="Probability [0-1] that a given unit is an injected anomaly pattern.",
    )
    parser.add_argument(
        "--duration", type=int, default=gen_cfg["duration_seconds"],
        help="How long to run in seconds. 0 or negative means run until interrupted (Ctrl+C).",
    )
    parser.add_argument(
        "--num-accounts", type=int, default=500, help="Size of the synthetic account pool."
    )
    parser.add_argument("--seed", type=int, default=gen_cfg.get("seed"), help="Random seed for reproducibility.")
    args = parser.parse_args()

    logger.info(
        "Starting generator: rate=%.2f/s anomaly_ratio=%.3f duration=%ss accounts=%d seed=%s",
        args.rate, args.anomaly_ratio, args.duration, args.num_accounts, args.seed,
    )

    stats = run_producer_loop(
        rate_per_second=args.rate,
        duration_seconds=args.duration,
        anomaly_ratio=args.anomaly_ratio,
        num_accounts=args.num_accounts,
        seed=args.seed,
    )

    print(f"Generator finished: {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
