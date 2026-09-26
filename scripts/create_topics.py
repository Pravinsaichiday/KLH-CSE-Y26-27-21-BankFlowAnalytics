#!/usr/bin/env python3
"""
CLI: Idempotently create all BankFlow Kafka topics via the Kafka Admin API.

Usage:
    python scripts/create_topics.py
    python scripts/create_topics.py --list
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.common.logging_config import get_logger  # noqa: E402
from src.kafka.topics import create_topics_idempotent, list_topics  # noqa: E402

logger = get_logger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create/list BankFlow Kafka topics.")
    parser.add_argument(
        "--list", action="store_true", help="List existing topics instead of creating."
    )
    args = parser.parse_args()

    try:
        if args.list:
            topics = list_topics()
            print("Existing topics:")
            for t in topics:
                print(f"  - {t}")
        else:
            create_topics_idempotent()
            print("Topic creation complete (idempotent).")
        return 0
    except Exception as exc:  # pragma: no cover
        logger.error("create_topics.py failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
