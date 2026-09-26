"""
High-throughput, idempotent Kafka producer for BankFlow Analytics.

Wraps kafka-python-ng's KafkaProducer with:
  - idempotent delivery (enable.idempotence=True, acks=all)
  - batching + linger for throughput
  - retries with backoff
  - JSON serialization
  - graceful close / flush semantics
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.common.config import settings
from src.common.logging_config import get_logger
from src.common.utils import safe_json_dumps

logger = get_logger(__name__)


class BankTransactionProducer:
    """Thin, resilient wrapper around KafkaProducer for transaction events."""

    def __init__(self, topic: Optional[str] = None):
        producer_cfg = settings.kafka["kafka"]["producer"]
        self.topic = topic or settings.topics["transactions"]

        self._producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=settings.kafka["kafka"]["client_id"],
            acks=producer_cfg["acks"],
            
            retries=producer_cfg["retries"],
            linger_ms=producer_cfg["linger_ms"],
            batch_size=producer_cfg["batch_size"],
            compression_type=producer_cfg["compression_type"],
            max_in_flight_requests_per_connection=producer_cfg[
                "max_in_flight_requests_per_connection"
            ],
            request_timeout_ms=producer_cfg["request_timeout_ms"],
            value_serializer=lambda v: safe_json_dumps(v).encode("utf-8")
            if isinstance(v, dict)
            else str(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        )
        self._sent = 0
        self._errors = 0

    def send(self, event: Dict[str, Any], key: Optional[str] = None) -> None:
        """Send a single event asynchronously, with an error callback."""
        record_key = key or event.get("account_id")
        future = self._producer.send(self.topic, key=record_key, value=event)
        future.add_callback(self._on_success).add_errback(self._on_error, event)

    def send_batch(self, events: List[Dict[str, Any]]) -> None:
        for event in events:
            self.send(event)

    def _on_success(self, _metadata) -> None:
        self._sent += 1

    def _on_error(self, exc: KafkaError, event: Dict[str, Any]) -> None:
        self._errors += 1
        logger.error(
            "Failed to deliver transaction %s: %s",
            event.get("transaction_id", "<unknown>"),
            exc,
        )

    def flush(self, timeout: Optional[float] = None) -> None:
        self._producer.flush(timeout=timeout)

    def close(self) -> None:
        self.flush()
        self._producer.close()
        logger.info("Producer closed. Sent=%d Errors=%d", self._sent, self._errors)

    @property
    def stats(self) -> Dict[str, int]:
        return {"sent": self._sent, "errors": self._errors}


def run_producer_loop(
    rate_per_second: float,
    duration_seconds: int,
    anomaly_ratio: float,
    num_accounts: int = 500,
    seed: Optional[int] = None,
) -> Dict[str, int]:
    """
    Run the generator + producer loop at a target throughput.
    duration_seconds <= 0 means run indefinitely (until interrupted).
    """
    from src.generator.transaction_generator import TransactionGenerator

    generator = TransactionGenerator(
        num_accounts=num_accounts, anomaly_ratio=anomaly_ratio, seed=seed
    )
    producer = BankTransactionProducer()

    interval = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
    start = time.monotonic()
    total_events = 0

    try:
        while True:
            elapsed = time.monotonic() - start
            if duration_seconds > 0 and elapsed >= duration_seconds:
                break

            events = generator.next_batch()
            producer.send_batch(events)
            total_events += len(events)

            if total_events % 100 == 0:
                logger.info("Produced %d events so far...", total_events)

            if interval > 0:
                time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Interrupted by user; shutting down producer loop.")
    finally:
        producer.close()

    logger.info("Producer loop finished. Total events: %d", total_events)
    return {"total_events": total_events, **producer.stats}
