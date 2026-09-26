"""
Centralized configuration loader for BankFlow Analytics.

Loads YAML configs from the `config/` directory and merges in environment
variables (via python-dotenv) so every module reads settings from a single
source of truth instead of hardcoding values.
"""

from __future__ import annotations

import os
import functools
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

# Resolve repo root relative to this file: src/common/config.py -> repo root
REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"

load_dotenv(REPO_ROOT / ".env", override=False)


class ConfigError(Exception):
    """Raised when a required configuration key or file is missing."""


def _load_yaml(filename: str) -> Dict[str, Any]:
    path = CONFIG_DIR / filename
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data


@functools.lru_cache(maxsize=None)
def load_app_config() -> Dict[str, Any]:
    return _load_yaml("app.yaml")


@functools.lru_cache(maxsize=None)
def load_kafka_config() -> Dict[str, Any]:
    return _load_yaml("kafka.yaml")


@functools.lru_cache(maxsize=None)
def load_anomaly_config() -> Dict[str, Any]:
    return _load_yaml("anomaly.yaml")


@functools.lru_cache(maxsize=None)
def load_schema_config() -> Dict[str, Any]:
    return _load_yaml("schema.yaml")


def get_env(key: str, default: Any = None, required: bool = False) -> Any:
    """Fetch an environment variable, optionally enforcing presence."""
    value = os.environ.get(key, default)
    if required and (value is None or value == ""):
        raise ConfigError(f"Required environment variable '{key}' is not set.")
    return value


def resolve_path(relative_path: str) -> Path:
    """Resolve a config-declared relative path against the repo root."""
    p = Path(relative_path)
    if p.is_absolute():
        return p
    return (REPO_ROOT / p).resolve()


class Settings:
    """
    Convenience façade bundling all configuration sections together.
    Instantiate once per process: `settings = Settings()`.
    """

    def __init__(self) -> None:
        self.app = load_app_config()
        self.kafka = load_kafka_config()
        self.anomaly = load_anomaly_config()
        self.schema = load_schema_config()

        # Kafka bootstrap servers: env var wins over YAML default.
        self.kafka_bootstrap_servers = get_env(
            "KAFKA_BOOTSTRAP_SERVERS",
            self.kafka["kafka"]["bootstrap_servers"],
        )

        storage = self.app["storage"]
        self.bronze_path = resolve_path(get_env("BRONZE_PATH", storage["bronze_path"]))
        self.silver_path = resolve_path(get_env("SILVER_PATH", storage["silver_path"]))
        self.quarantine_path = resolve_path(
            get_env("QUARANTINE_PATH", storage["quarantine_path"])
        )
        self.checkpoint_base_path = resolve_path(
            get_env("CHECKPOINT_BASE_PATH", storage["checkpoint_base_path"])
        )
        self.gold_sqlite_path = resolve_path(
            get_env("GOLD_SQLITE_PATH", storage["gold_sqlite_path"])
        )

    @property
    def topics(self) -> Dict[str, str]:
        t = self.kafka["kafka"]["topics"]
        return {
            "transactions": t["transactions"]["name"],
            "quarantine": t["quarantine"]["name"],
            "anomaly": t["anomaly"]["name"],
        }


settings = Settings()
