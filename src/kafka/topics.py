"""
Kafka topic management (idempotent create/describe) for BankFlow Analytics.
"""

from __future__ import annotations

from typing import Dict, List

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from src.common.config import settings
from src.common.logging_config import get_logger

logger = get_logger(__name__)


def _topic_specs() -> List[Dict]:
    topics_cfg = settings.kafka["kafka"]["topics"]
    return [
        {
            "name": spec["name"],
            "num_partitions": spec["partitions"],
            "replication_factor": spec["replication_factor"],
        }
        for spec in topics_cfg.values()
    ]


def get_admin_client() -> KafkaAdminClient:
    return KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=f"{settings.kafka['kafka']['client_id']}-admin",
    )


def create_topics_idempotent() -> None:
    """Create all required topics if they don't already exist. Safe to re-run."""
    admin = get_admin_client()
    specs = _topic_specs()

    new_topics = [
        NewTopic(
            name=spec["name"],
            num_partitions=spec["num_partitions"],
            replication_factor=spec["replication_factor"],
        )
        for spec in specs
    ]

    try:
        existing = set(admin.list_topics())
    except Exception as exc:  # pragma: no cover - connectivity issue
        logger.error("Could not list existing Kafka topics: %s", exc)
        existing = set()

    to_create = [t for t in new_topics if t.name not in existing]

    if not to_create:
        logger.info("All BankFlow topics already exist: %s", [s["name"] for s in specs])
        admin.close()
        return

    try:
        admin.create_topics(new_topics=to_create, validate_only=False)
        logger.info("Created topics: %s", [t.name for t in to_create])
    except TopicAlreadyExistsError:
        logger.info("Topics already existed (race with another creator).")
    except Exception as exc:  # pragma: no cover
        logger.error("Failed creating topics: %s", exc)
        raise
    finally:
        admin.close()


def list_topics() -> List[str]:
    admin = get_admin_client()
    try:
        return sorted(admin.list_topics())
    finally:
        admin.close()
