"""
Wikimedia EventStreams -> Kafka producer.

Subscribes to Wikimedia's Server-Sent Events (SSE) feed of recent changes,
filters to human edits, and publishes JSON events to a Kafka topic for
downstream Flink processing.

Run locally:
    source .venv/bin/activate
    python producer/producer.py

Stop with Ctrl-C (graceful shutdown: flushes in-flight messages first).
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
from typing import Any

from confluent_kafka import Producer
from sseclient import SSEClient

# ═══════════════════════════════════════════════════════════════════════
# CONFIG (env-overridable so the same code works locally and in docker)
# ═══════════════════════════════════════════════════════════════════════
WMF_URL = os.getenv(
    "WMF_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
# localhost:9092 from your Mac, kafka:29092 from inside docker. Env var picks.
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "wmf.edits.raw")

# Wikimedia requires a descriptive User-Agent identifying your tool and contact.
# Anonymous requests get 403'd. Customize the URL/handle in your fork.
# See: https://meta.wikimedia.org/wiki/User-Agent_policy
USER_AGENT = os.getenv(
    "USER_AGENT",
    "streamscope/0.1 (https://github.com/yourname/streamscope; portfolio project)",
)

# ═══════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")

STATS = {"produced": 0, "filtered": 0, "errors": 0}


def on_delivery(err: Any, msg: Any) -> None:
    """Async callback from librdkafka when a produce request completes."""
    if err is not None:
        STATS["errors"] += 1
        log.error("delivery failed: %s", err)
        return

    STATS["produced"] += 1
    if STATS["produced"] % 100 == 0:
        log.info(
            "produced=%d filtered=%d errors=%d",
            STATS["produced"], STATS["filtered"], STATS["errors"],
        )


def make_producer() -> Producer:
    """Kafka producer with production-shape defaults."""
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "wmf-producer",
        "linger.ms": 50,                 # batch up to 50ms for throughput
        "compression.type": "snappy",    # ~3x storage savings on JSON
        "enable.idempotence": True,      # prevent duplicates on retry
        "retries": 10,
        "retry.backoff.ms": 100,
    })


def should_keep(payload: dict) -> bool:
    """Business filter — cheaper to drop here than in Kafka/Flink."""
    if payload.get("type") != "edit":   # drop "new", "log", "categorize", etc.
        return False
    if payload.get("bot"):              # drop bots (30-50% of stream, skew analytics)
        return False
    return True


def main() -> int:
    log.info("connecting to Kafka at %s", KAFKA_BOOTSTRAP)
    log.info("topic=%s  source=%s", TOPIC, WMF_URL)
    log.info("user-agent=%s", USER_AGENT)
    producer = make_producer()

    # ─── Graceful shutdown: flush in-flight messages before exit ─────────
    def _shutdown(_signum, _frame):
        log.info("shutting down — flushing remaining messages...")
        producer.flush(timeout=10)
        log.info(
            "final stats: produced=%d filtered=%d errors=%d",
            STATS["produced"], STATS["filtered"], STATS["errors"],
        )
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ─── Main loop: read SSE, filter, produce ───────────────────────────
    # SSEClient handles reconnects automatically when connection drops
    # (which Wikimedia does ~every 15 min). Sends Last-Event-ID to resume.
    # Pass User-Agent through; Wikimedia requires it.
    log.info("subscribing to Wikimedia EventStreams...")
    for event in SSEClient(WMF_URL, headers={"User-Agent": USER_AGENT}):
        if not event.data:
            continue

        try:
            payload = json.loads(event.data)
        except json.JSONDecodeError as e:
            log.warning("skipping malformed event: %s", e)
            continue

        if not should_keep(payload):
            STATS["filtered"] += 1
            continue

        # Partition key = wiki server name → preserves per-wiki ordering.
        key = payload["server_name"].encode("utf-8")
        value = json.dumps(payload, separators=(",", ":")).encode("utf-8")

        producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)

        # poll(0) is non-blocking — services completed delivery callbacks.
        # Required pattern: without it, callback queue grows unbounded.
        producer.poll(0)

    producer.flush(timeout=10)
    return 0


if __name__ == "__main__":
    sys.exit(main())
