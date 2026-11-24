"""Helpers for the Airflow replay experiment (Week 7): prove that replayed events reach PostgreSQL, scored."""
from __future__ import annotations

import csv
import os
import time
from pathlib import Path


def read_first_ids(csv_path: Path, n: int) -> list[str]:
    """transaction_id of the first `n` data rows of a processed split (the rows a replay with --limit n sends)."""
    ids = []
    with Path(csv_path).open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            ids.append(row["transaction_id"])
            if len(ids) >= n:
                break
    return ids


def _connect():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "fraud"),
        user=os.getenv("PGUSER", "fraud"),
        password=os.getenv("PGPASSWORD", "fraud"),
        connect_timeout=5,
    )


def scored_among(ids: list[str]) -> int:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM transactions WHERE transaction_id = ANY(%s) AND fraud_score IS NOT NULL",
                        (ids,))
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def wait_until_scored(ids: list[str], timeout_s: float = 180.0, poll_s: float = 3.0) -> dict:
    """Wait until every id is in PostgreSQL with a model score; raise on timeout (the Airflow task then fails)."""
    start = time.monotonic()
    found = scored_among(ids)
    while found < len(ids) and time.monotonic() - start < timeout_s:
        time.sleep(poll_s)
        found = scored_among(ids)
    result = {"expected": len(ids), "scored_in_postgres": found, "seconds": round(time.monotonic() - start, 1)}
    if found < len(ids):
        raise RuntimeError(f"replay incomplete: {result}")
    return result


def check_services() -> None:
    """Fail fast if Kafka or PostgreSQL is not reachable."""
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                             consumer_timeout_ms=3000)
    try:
        topics = consumer.topics()
    finally:
        consumer.close()
    topic = os.getenv("KAFKA_TOPIC", "transactions.raw")
    if topic not in topics:
        raise RuntimeError(f"Kafka topic {topic} not found")
    conn = _connect()
    conn.close()
