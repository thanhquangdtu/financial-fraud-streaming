"""Read the dashboard tables from PostgreSQL (written by the Spark streaming job, Week 4).

Connection (defaults match docker-compose.yml): PGHOST=localhost PGPORT=5432 PGDATABASE=fraud
PGUSER=fraud PGPASSWORD=fraud
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]


def _query(conn, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def _connect():
    import psycopg2  # imported lazily so mock mode works without the driver

    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "fraud"),
        user=os.getenv("PGUSER", "fraud"),
        password=os.getenv("PGPASSWORD", "fraud"),
        connect_timeout=3,
    )


def inspect_parquet_lake() -> dict:
    """Inspect Parquet Data Lake files on disk (data/lake/transactions)."""
    lake_dir = ROOT / "data" / "lake" / "transactions"
    if not lake_dir.exists():
        return {"exists": False, "files": 0, "size_kb": 0.0, "partitions": []}
    parquet_files = list(lake_dir.rglob("*.parquet"))
    total_bytes = sum(f.stat().st_size for f in parquet_files if f.is_file())
    partitions = sorted(
        [p.name.replace("event_date=", "") for p in lake_dir.iterdir() if p.is_dir() and p.name.startswith("event_date=")]
    )
    return {
        "exists": True,
        "files": len(parquet_files),
        "size_kb": round(total_bytes / 1024.0, 2),
        "partitions": partitions,
    }


def load_pipeline() -> dict:
    """Totals and data-quality figures taken straight from the database (not limited to a sample)."""
    conn = _connect()
    try:
        c = _query(conn, """
            SELECT (SELECT count(*) FROM transactions) AS tx,
                   (SELECT count(*) FROM daily_stats) AS daily_rows,
                   (SELECT count(*) FROM customer_week) AS week_rows,
                   (SELECT count(*) FROM dead_letter) AS dead_letter,
                   (SELECT count(DISTINCT customer_id) FROM transactions) AS customers,
                   (SELECT count(DISTINCT merchant_id) FROM transactions) AS merchants,
                   (SELECT coalesce(sum(amount), 0)::float8 FROM transactions) AS amount,
                   (SELECT coalesce(sum(fraud_label), 0) FROM transactions) AS fraud_labeled,
                   (SELECT min(event_time) FROM transactions) AS time_min,
                   (SELECT max(event_time) FROM transactions) AS time_max,
                   (SELECT min(step) FROM transactions) AS step_min,
                   (SELECT max(step) FROM transactions) AS step_max,
                   (SELECT max(ingestion_time) FROM transactions) AS last_ingestion,
                   (SELECT coalesce(sum(tx_count), 0) FROM daily_stats) AS daily_sum,
                   (SELECT coalesce(sum(tx_count), 0) FROM customer_week) AS week_sum
        """).iloc[0].to_dict()
        by_source = _query(conn, "SELECT data_source, count(*) AS tx_count, coalesce(sum(amount),0)::float8 AS total_amount FROM transactions GROUP BY 1 ORDER BY 2 DESC")
        by_step = _query(conn, """
            SELECT step, min(event_time) AS event_time, count(*) AS tx_count,
                   sum(amount)::float8 AS total_amount, coalesce(sum(fraud_label), 0) AS fraud_labeled
            FROM transactions GROUP BY step ORDER BY step""")
        top_merchants = _query(conn, """
            SELECT merchant_id, category, count(*) AS tx_count,
                   sum(amount)::float8 AS total_amount,
                   coalesce(sum(fraud_label), 0) AS fraud_count
            FROM transactions
            GROUP BY merchant_id, category
            ORDER BY tx_count DESC
            LIMIT 12""")
        by_age = _query(conn, """
            SELECT coalesce(nullif(age_group, ''), 'U') AS age_group,
                   count(*) AS tx_count,
                   coalesce(sum(fraud_label), 0) AS fraud_count
            FROM transactions
            GROUP BY 1 ORDER BY 1""")
        by_gender = _query(conn, """
            SELECT coalesce(nullif(gender, ''), 'U') AS gender,
                   count(*) AS tx_count,
                   coalesce(sum(fraud_label), 0) AS fraud_count
            FROM transactions
            GROUP BY 1 ORDER BY 2 DESC""")
        dead = _query(conn, "SELECT id, left(raw, 300) AS raw, received_at FROM dead_letter "
                            "ORDER BY received_at DESC LIMIT 100")
    finally:
        conn.close()
    return {
        "counts": c,
        "by_source": by_source,
        "by_step": by_step,
        "top_merchants": top_merchants,
        "by_age": by_age,
        "by_gender": by_gender,
        "dead_letter": dead,
        "parquet": inspect_parquet_lake(),
    }


def load_from_postgres(limit: int = 20000):
    """Return (transactions, daily_stats, customer_week) DataFrames from the real database."""
    conn = _connect()
    try:
        transactions = _query(conn, f"""
            SELECT transaction_id, event_time, ingestion_time, customer_id, merchant_id, category,
                   amount::float8 AS amount, step, age_group, gender, data_source,
                   COALESCE(fraud_label, 0) AS fraud_label
            FROM transactions ORDER BY event_time DESC, transaction_id LIMIT {int(limit)}""")
        daily_stats = _query(conn, """
            SELECT window_start, window_end, category, tx_count,
                   total_amount::float8 AS total_amount, avg_amount::float8 AS avg_amount
            FROM daily_stats ORDER BY window_start DESC, category""")
        customer_week = _query(conn, """
            SELECT window_start, window_end, customer_id, tx_count,
                   total_amount::float8 AS total_amount, avg_amount::float8 AS avg_amount, distinct_categories
            FROM customer_week ORDER BY window_start DESC, total_amount DESC""")
    finally:
        conn.close()

    transactions["event_time"] = pd.to_datetime(transactions["event_time"], utc=True)
    transactions["fraud_prediction"] = transactions["fraud_label"].astype(int)
    transactions["fraud_score"] = transactions["fraud_prediction"].astype(float)
    transactions["status"] = transactions["fraud_prediction"].map({1: "FLAGGED", 0: "NORMAL"})

    # Compute data-driven alert reasons so users/instructors can see WHY each transaction is suspicious
    cat_avg = transactions.groupby("category")["amount"].transform("mean").replace(0, 1.0).round(2)
    transactions["category_avg"] = cat_avg
    ratio = transactions["amount"] / cat_avg
    cust_freq = transactions.groupby(["customer_id", "step"])["transaction_id"].transform("count")
    fraud_merchants = set(
        transactions[transactions["fraud_prediction"] == 1]["merchant_id"]
        .value_counts()
        .loc[lambda s: s >= 2]
        .index
    )

    reasons = []
    for row, c_avg, r_val, c_cnt in zip(transactions.itertuples(index=False), cat_avg, ratio, cust_freq):
        if row.fraud_prediction == 0:
            reasons.append(f"Bình thường (TB ngành: ${c_avg:,.2f})")
            continue
        tags = []
        if row.amount >= 500 or r_val >= 2.0:
            tags.append(f"Số tiền cao (${row.amount:,.2f} vs TB ngành ${c_avg:,.2f} · gấp {r_val:.1f}x)")
        else:
            tags.append(f"TB ngành: ${c_avg:,.2f} ({r_val:.1f}x)")
        if row.merchant_id in fraud_merchants:
            tags.append(f"Merchant điểm nóng ({row.merchant_id})")
        if c_cnt >= 2:
            tags.append(f"Quẹt liên tiếp ({c_cnt} lần/ngày)")
        if row.category in ("es_travel", "es_sportsandtoys", "es_hotelservices", "es_health", "es_wellnessandbeauty"):
            tags.append(f"Ngành nhạy cảm ({row.category.replace('es_', '')})")
        reasons.append(" · ".join(tags))

    transactions["alert_reason"] = reasons
    transactions = transactions.drop(columns=["fraud_label"])
    for col in ("window_start", "window_end"):
        daily_stats[col] = pd.to_datetime(daily_stats[col], utc=True)
        customer_week[col] = pd.to_datetime(customer_week[col], utc=True)
    return transactions, daily_stats, customer_week
