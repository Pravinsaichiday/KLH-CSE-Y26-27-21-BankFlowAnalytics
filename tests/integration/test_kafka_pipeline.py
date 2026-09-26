"""
Integration tests for the Kafka producer/topic layer and the raw Kafka ->
Spark parsing path.

These tests require a running Kafka broker reachable at
KAFKA_BOOTSTRAP_SERVERS (see docker/docker-compose.yml). They are skipped
automatically if no broker is reachable, so the unit-test suite (which does
not require external services) still runs cleanly in CI without Kafka.
"""

from __future__ import annotations

import socket
import time

import pytest

from src.common.config import settings


def _kafka_reachable() -> bool:
    host, _, port = settings.kafka_bootstrap_servers.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _kafka_reachable(), reason="Kafka broker not reachable; skipping integration test."
)


def test_create_topics_idempotent_runs_twice_without_error():
    from src.kafka.topics import create_topics_idempotent, list_topics

    create_topics_idempotent()
    create_topics_idempotent()  # must be safe to re-run

    topics = list_topics()
    for expected in settings.topics.values():
        assert expected in topics


def test_producer_sends_and_flushes_events():
    from src.kafka.producer import BankTransactionProducer
    from src.generator.transaction_generator import TransactionGenerator

    generator = TransactionGenerator(num_accounts=5, anomaly_ratio=0.0, seed=1)
    producer = BankTransactionProducer()

    events = generator.stream(count=10)
    producer.send_batch(events)
    producer.flush(timeout=10)

    assert producer.stats["sent"] == len(events)
    assert producer.stats["errors"] == 0
    producer.close()


def test_end_to_end_produce_and_consume_roundtrip():
    """Produce a known transaction, then consume it back with a plain KafkaConsumer."""
    from kafka import KafkaConsumer
    from src.kafka.producer import BankTransactionProducer
    from src.common.utils import safe_json_loads

    producer = BankTransactionProducer()
    marker_id = f"TXN-INTEGRATION-{int(time.time())}"
    event = {
        "transaction_id": marker_id,
        "account_id": "ACC-TEST",
        "transaction_type": "TRANSFER",
        "amount": 42.0,
        "currency": "INR",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "channel": "MOBILE",
        "status": "SUCCESS",
        "merchant_category": "OTHER",
        "city": "Mumbai",
        "device_id": "DEV-TEST",
        "source": "integration-test",
    }
    producer.send(event)
    producer.flush(timeout=10)
    producer.close()

    consumer = KafkaConsumer(
        settings.topics["transactions"],
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=15000,
    )

    found = False
    for message in consumer:
        payload = safe_json_loads(message.value)
        if payload and payload.get("transaction_id") == marker_id:
            found = True
            break

    consumer.close()
    assert found, "Produced transaction was not found in the topic on consume."
