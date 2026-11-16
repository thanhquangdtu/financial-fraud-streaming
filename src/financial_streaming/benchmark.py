"""End-to-end streaming benchmark (Week 6): Producer -> Kafka -> Spark (ML scoring) -> PostgreSQL.

Usage:
    .\.venv\Scripts\python.exe -m financial_streaming.benchmark

Measures 3 load levels (default: 50, 200, 500 events/s):
  - producer_rate_eps       : actual Kafka ingestion throughput
  - e2e_seconds             : total wall time until all N new events are ML-scored in PostgreSQL
  - e2e_throughput_eps      : end-to-end throughput (N / e2e_seconds)
  - spark_sync_latency_sec  : time between producer completion and Spark+PostgreSQL commit completion
  - data_loss               : 0 if every sent transaction is present and scored in PostgreSQL
Writes results to data/reports/benchmark_report.json.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from .preprocessing.common import ROOT, write_json
from .producer.config import load_config
from .producer.replay import load_checkpoint, replay, write_control
from .producer.sink import KafkaSink


def _pg_connect():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "fraud"),
        user=os.getenv("PGUSER", "fraud"),
        password=os.getenv("PGPASSWORD", "fraud"),
        connect_timeout=5,
    )


def _scored_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM transactions WHERE fraud_score IS NOT NULL")
        return int(cur.fetchone()[0])


# ==============================================================================
# Hàm: run_single_load
# - Mục đích: Thực hiện đo kiểm 1 mức tải (rate events/giây) từ Producer -> Kafka -> Spark ML -> PostgreSQL.
# - Điều cần chú ý:
#   + Đếm số dòng đã chấm điểm ML trong PostgreSQL trước khi bắn (`before_scored`).
#   + Bắn `events` dòng mới từ vị trí checkpoint hiện tại vào Kafka.
#   + Chờ và đo thời gian đến khi Spark xử lý và ghi đủ `before_scored + sent` dòng vào PostgreSQL.
# ==============================================================================
def run_single_load(conn, rate: float, events: int, timeout_s: float = 60.0) -> dict:
    cfg = load_config()
    cfg = type(cfg)(
        bootstrap_servers=cfg.bootstrap_servers,
        topic=cfg.topic,
        rate=float(rate),
        input_path=ROOT / "data/processed/validation.csv",
    )
    control_file = ROOT / "data/replay/control"
    checkpoint_file = ROOT / "data/replay/checkpoint.json"
    write_control(control_file, "run")

    before_scored = _scored_count(conn)
    cp = load_checkpoint(checkpoint_file, cfg.input_path, cfg.topic)
    start_idx = int(cp["next_index"]) if cp else 0
    if start_idx < before_scored:
        # Advance checkpoint to match what is already stored in PostgreSQL so every benchmark row is new
        checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_file.write_text(
            json.dumps({"input": str(cfg.input_path), "topic": cfg.topic, "next_index": before_scored, "completed": False}),
            encoding="utf-8",
        )

    sink = KafkaSink(cfg.bootstrap_servers)
    t0 = time.perf_counter()
    try:
        prod_summary = replay(
            cfg,
            sink,
            input_path=cfg.input_path,
            control_file=control_file,
            checkpoint_file=checkpoint_file,
            limit=events,
        )
    finally:
        sink.close()
    t_prod = time.perf_counter()
    sent = int(prod_summary["sent"])
    target_scored = before_scored + sent

    # Poll PostgreSQL until Spark commits all newly sent & ML-scored transactions
    current_scored = _scored_count(conn)
    while current_scored < target_scored and (time.perf_counter() - t_prod) < timeout_s:
        time.sleep(0.25)
        current_scored = _scored_count(conn)
    t_end = time.perf_counter()

    prod_sec = max(t_prod - t0, 1e-6)
    e2e_sec = max(t_end - t0, 1e-6)
    sync_latency = max(t_end - t_prod, 0.0)
    processed_delta = max(0, current_scored - before_scored)
    return {
        "target_rate_eps": float(rate),
        "events_sent": sent,
        "events_scored_in_pg": processed_delta,
        "data_loss": max(0, sent - processed_delta),
        "producer_seconds": round(prod_sec, 3),
        "producer_rate_eps": round(sent / prod_sec, 2),
        "e2e_seconds": round(e2e_sec, 3),
        "e2e_throughput_eps": round(processed_delta / e2e_sec, 2),
        "spark_sync_latency_sec": round(sync_latency, 3),
        "passed": processed_delta == sent,
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--rates", type=float, nargs="+", default=[50.0, 200.0, 500.0])
    ap.add_argument("--events", type=int, nargs="+", default=[150, 300, 500])
    a = ap.parse_args(argv)

    if len(a.events) != len(a.rates):
        a.events = [a.events[0]] * len(a.rates)

    conn = _pg_connect()
    try:
        scenarios = []
        for r, n in zip(a.rates, a.events):
            print(f"Running benchmark: target_rate={r} eps, events={n} ...")
            res = run_single_load(conn, rate=r, events=n)
            print(" ->", res)
            scenarios.append(res)
    finally:
        conn.close()

    report = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "pipeline": "Producer -> Kafka (3 partitions) -> Spark 3.5.3 (Dedup + 100-tree GBT ML Scoring) -> PostgreSQL 16",
        "spark_trigger_interval": "5 seconds",
        "scenarios": scenarios,
        "all_passed": all(s["passed"] for s in scenarios),
    }
    out = ROOT / "data/reports/benchmark_report.json"
    write_json(out, report)
    print(f"Benchmark report saved to {out}")


if __name__ == "__main__":
    main()
