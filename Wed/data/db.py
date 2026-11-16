"""Read the dashboard tables from PostgreSQL (written by the Spark streaming job, Week 4).

Connection (defaults match docker-compose.yml): PGHOST=localhost PGPORT=5432 PGDATABASE=fraud
PGUSER=fraud PGPASSWORD=fraud
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
_HIST_CACHE: dict | None = None


def _customer_history() -> tuple[dict[str, float], dict[str, set[str]], dict[str, float], float]:
    """Load each customer's historical normal baseline, visited categories & merchant fraud rates from train.csv."""
    global _HIST_CACHE
    if _HIST_CACHE is not None and "merch_fraud" in _HIST_CACHE:
        return _HIST_CACHE["avg"], _HIST_CACHE["cats"], _HIST_CACHE["merch_fraud"], _HIST_CACHE["global_avg"]
    train_path = ROOT / "data" / "processed" / "train.csv"
    if not train_path.is_file():
        _HIST_CACHE = {"avg": {}, "cats": {}, "merch_fraud": {}, "global_avg": 37.89}
        return {}, {}, {}, 37.89
    tr = pd.read_csv(train_path, usecols=["customer_id", "merchant_id", "amount", "category", "fraud_label"])
    normal = tr[tr["fraud_label"] == 0]
    avg_map = normal.groupby("customer_id")["amount"].mean().round(2).to_dict()
    cats_map = normal.groupby("customer_id")["category"].agg(set).to_dict()
    merch_fraud = (tr.groupby("merchant_id")["fraud_label"].mean() * 100.0).round(1).to_dict()
    global_avg = round(float(normal["amount"].mean()), 2)
    _HIST_CACHE = {"avg": avg_map, "cats": cats_map, "merch_fraud": merch_fraud, "global_avg": global_avg}
    return avg_map, cats_map, merch_fraud, global_avg


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
                   (SELECT coalesce(sum(fraud_prediction), 0) FROM transactions) AS fraud_labeled,
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
                   sum(amount)::float8 AS total_amount, coalesce(sum(fraud_prediction), 0) AS fraud_labeled
            FROM transactions GROUP BY step ORDER BY step""")
        top_merchants = _query(conn, """
            SELECT merchant_id, category, count(*) AS tx_count,
                   sum(amount)::float8 AS total_amount,
                   coalesce(sum(fraud_prediction), 0) AS fraud_count
            FROM transactions
            GROUP BY merchant_id, category
            ORDER BY tx_count DESC
            LIMIT 12""")
        by_age = _query(conn, """
            SELECT coalesce(nullif(age_group, ''), 'U') AS age_group,
                   count(*) AS tx_count,
                   coalesce(sum(fraud_prediction), 0) AS fraud_count
            FROM transactions
            GROUP BY 1 ORDER BY 1""")
        by_gender = _query(conn, """
            SELECT coalesce(nullif(gender, ''), 'U') AS gender,
                   count(*) AS tx_count,
                   coalesce(sum(fraud_prediction), 0) AS fraud_count
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
                   COALESCE(fraud_prediction, 0) AS fraud_prediction, COALESCE(fraud_score, 0)::float8 AS fraud_score, COALESCE(fraud_label, 0) AS actual_label
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
    transactions["fraud_prediction"] = transactions["fraud_prediction"].astype(int)
    transactions["fraud_score"] = transactions["fraud_score"].astype(float).round(4)
    transactions["status"] = [
        "NORMAL" if pred == 0 else ("BLOCKED" if score >= 0.80 else "FLAGGED")
        for pred, score in zip(transactions["fraud_prediction"], transactions["fraud_score"])
    ]

    # Compute per-customer historical normal baseline from train.csv (125 days of history) + normal stream rows
    hist_avg_map, hist_cats_map, merch_fraud_map, global_normal_avg = _customer_history()
    normal_mask = transactions["fraud_prediction"] == 0
    stream_normal_avg = transactions[normal_mask].groupby("customer_id")["amount"].mean().round(2).to_dict()
    cust_baseline_map = {**stream_normal_avg, **hist_avg_map}

    cust_avg = transactions["customer_id"].map(cust_baseline_map).fillna(global_normal_avg).replace(0, 1.0).round(2)
    cat_avg = transactions.groupby("category")["amount"].transform("mean").replace(0, 1.0).round(2)
    transactions["cust_avg"] = cust_avg
    transactions["category_avg"] = cat_avg

    cust_ratio = transactions["amount"] / cust_avg
    cat_ratio = transactions["amount"] / cat_avg
    cust_freq = transactions.groupby(["customer_id", "step"])["transaction_id"].transform("count")
    fraud_merchants = set(
        transactions[transactions["fraud_prediction"] == 1]["merchant_id"]
        .value_counts()
        .loc[lambda s: s >= 2]
        .index
    )

    reasons = []
    for row, u_avg, u_rat, c_avg, c_rat, c_cnt in zip(
        transactions.itertuples(index=False), cust_avg, cust_ratio, cat_avg, cat_ratio, cust_freq
    ):
        known_cats = hist_cats_map.get(row.customer_id, set())
        m_rate = merch_fraud_map.get(row.merchant_id, 0.0)
        if row.fraud_prediction == 0:
            reasons.append(f"Đúng thói quen khách (TB khách: ${u_avg:,.2f})")
            continue
        tags = []
        if u_rat >= 1.5:
            tags.append(f"Vọt gấp {u_rat:.1f}x TB thường ngày của khách (${row.amount:,.2f} vs TB khách ${u_avg:,.2f})")
        elif c_rat >= 1.5 or row.amount >= 300:
            tags.append(f"Vượt chuẩn ngành {c_rat:.1f}x (${row.amount:,.2f} vs TB ngành ${c_avg:,.2f})")
        else:
            tags.append(f"Lệch hồ sơ khách (TB khách: ${u_avg:,.2f})")
        if known_cats and row.category not in known_cats:
            tags.append(f"Ngành mới lạ (chưa từng mua {row.category.replace('es_', '')} trong 125 ngày)")
        if c_cnt >= 2:
            tags.append(f"Tần suất đột biến ({c_cnt} GD/ngày)")
        if m_rate >= 10.0 or row.merchant_id in fraud_merchants:
            tags.append(f"Merchant rủi ro {row.merchant_id} ({m_rate:.1f}% GD lịch sử là gian lận)")
        reasons.append(" · ".join(tags))

    transactions["alert_reason"] = reasons
    for col in ("window_start", "window_end"):
        daily_stats[col] = pd.to_datetime(daily_stats[col], utc=True)
        customer_week[col] = pd.to_datetime(customer_week[col], utc=True)
    return transactions, daily_stats, customer_week
