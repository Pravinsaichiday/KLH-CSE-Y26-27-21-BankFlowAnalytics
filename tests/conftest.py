"""
Shared pytest fixtures for the BankFlow Analytics test suite.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest


@pytest.fixture(scope="session")
def spark():
    """A local, minimal SparkSession for unit-testing DataFrame transformations."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("BankFlowTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
