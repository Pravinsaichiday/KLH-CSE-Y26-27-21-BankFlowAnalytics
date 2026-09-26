#!/usr/bin/env python3
"""
CLI: Clean-state utility. Purges Delta logs (Bronze/Silver), quarantine
files, Spark checkpoints, and the Gold SQLite database so the pipeline can
be re-run from scratch.

Usage:
    python scripts/reset_data.py --all
    python scripts/reset_data.py --bronze --silver
    python scripts/reset_data.py --gold --yes   # skip confirmation prompt
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.common.config import settings  # noqa: E402
from src.common.logging_config import get_logger  # noqa: E402

logger = get_logger(__name__)


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
        logger.info("Removed directory: %s", path)
    elif path.exists():
        path.unlink()
        logger.info("Removed file: %s", path)
    else:
        logger.info("Nothing to remove at: %s (already clean)", path)


def reset_bronze() -> None:
    _remove_path(settings.bronze_path)
    _remove_path(settings.checkpoint_base_path / "bronze")


def reset_silver() -> None:
    _remove_path(settings.silver_path)
    _remove_path(settings.checkpoint_base_path / "silver")


def reset_quarantine() -> None:
    _remove_path(settings.quarantine_path)
    _remove_path(settings.checkpoint_base_path / "quarantine")


def reset_gold() -> None:
    _remove_path(settings.gold_sqlite_path)


def reset_scratch() -> None:
    _remove_path(REPO_ROOT / "data" / "_scratch")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset BankFlow Analytics data state.")
    parser.add_argument("--bronze", action="store_true", help="Purge the Bronze Delta layer.")
    parser.add_argument("--silver", action="store_true", help="Purge the Silver Delta layer.")
    parser.add_argument("--quarantine", action="store_true", help="Purge the quarantine Delta layer.")
    parser.add_argument("--gold", action="store_true", help="Purge the Gold SQLite database.")
    parser.add_argument("--scratch", action="store_true", help="Purge scratch/temp files.")
    parser.add_argument("--all", action="store_true", help="Purge everything (Bronze/Silver/Quarantine/Gold/scratch).")
    parser.add_argument("--yes", action="store_true", help="Skip the confirmation prompt.")
    args = parser.parse_args()

    if not any([args.bronze, args.silver, args.quarantine, args.gold, args.scratch, args.all]):
        parser.print_help()
        return 1

    targets = []
    if args.all or args.bronze:
        targets.append(("Bronze", reset_bronze))
    if args.all or args.silver:
        targets.append(("Silver", reset_silver))
    if args.all or args.quarantine:
        targets.append(("Quarantine", reset_quarantine))
    if args.all or args.gold:
        targets.append(("Gold (SQLite)", reset_gold))
    if args.all or args.scratch:
        targets.append(("Scratch", reset_scratch))

    print("The following will be permanently deleted:")
    for name, _ in targets:
        print(f"  - {name}")

    if not args.yes:
        confirm = input("Proceed? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return 1

    for name, fn in targets:
        logger.info("Resetting %s...", name)
        fn()

    print("Reset complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
