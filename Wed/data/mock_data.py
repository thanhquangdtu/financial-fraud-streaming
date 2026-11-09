"""Generate deterministic mock data matching the PostgreSQL dashboard schema."""
from __future__ import annotations

import numpy as np
import pandas as pd

CATEGORIES = [
    "es_transportation",
    "es_food",
    "es_health",
    "es_tech",
    "es_sportsandtoys",
    "es_travel",
    "es_fashion",
    "es_home",
    "es_leisure",
]


def generate_transactions(n: int = 800, seed: int = 42) -> pd.DataFrame:
    """Create mock transactions with the exact columns of `transactions`."""
    rng = np.random.default_rng(seed)
    now = pd.Timestamp.now(tz="UTC").floor("s")
    offsets = rng.integers(0, 7 * 24 * 60 * 60, size=n)
    event_times = now - pd.to_timedelta(offsets, unit="s")

    category = rng.choice(CATEGORIES, size=n, p=[.20, .19, .08, .08, .10, .07, .11, .09, .08])
    amount = np.round(rng.lognormal(mean=3.25, sigma=1.05, size=n), 2)
    # A small number of unusually large purchases makes the demo dashboard useful.
    large_idx = rng.choice(n, size=max(1, n // 35), replace=False)
    amount[large_idx] = np.round(rng.uniform(700, 6000, size=len(large_idx)), 2)

    fraud_prediction = (rng.random(n) < 0.055).astype(int)
    fraud_score = np.where(
        fraud_prediction == 1,
        rng.uniform(0.78, 0.9999, size=n),
        rng.uniform(0.001, 0.35, size=n),
    )
    status = np.where(
        fraud_prediction == 0,
        "NORMAL",
        np.where(fraud_score >= 0.96, "BLOCKED", "FLAGGED"),
    )

    customers = [f"C{value}" for value in rng.integers(1000000000, 1999999999, size=n)]
    merchants = [f"M{value}" for value in rng.integers(1000000000, 1999999999, size=n)]
    ids = [rng.bytes(12).hex() for _ in range(n)]

    df = pd.DataFrame({
        "transaction_id": ids,
        "event_time": event_times,
        "customer_id": customers,
        "merchant_id": merchants,
        "category": category,
        "amount": amount,
        "step": rng.integers(0, 180, size=n),
        "fraud_prediction": fraud_prediction,
        "fraud_score": np.round(fraud_score, 4),
        "status": status,
    })
    return df.sort_values("event_time", ascending=False).reset_index(drop=True)


def build_daily_stats(transactions: pd.DataFrame) -> pd.DataFrame:
    """Aggregate transactions into the `daily_stats` schema."""
    if transactions.empty:
        return pd.DataFrame(columns=[
            "window_start", "window_end", "category", "tx_count", "total_amount", "avg_amount"
        ])
    work = transactions.copy()
    work["window_start"] = pd.to_datetime(work["event_time"], utc=True).dt.floor("D")
    result = (
        work.groupby(["window_start", "category"], as_index=False)
        .agg(tx_count=("transaction_id", "count"),
             total_amount=("amount", "sum"),
             avg_amount=("amount", "mean"))
    )
    result["window_end"] = result["window_start"] + pd.Timedelta(days=1)
    result["total_amount"] = result["total_amount"].round(2)
    result["avg_amount"] = result["avg_amount"].round(4)
    return result[["window_start", "window_end", "category", "tx_count", "total_amount", "avg_amount"]].sort_values(
        ["window_start", "category"], ascending=[False, True]
    ).reset_index(drop=True)


def build_customer_week(transactions: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the latest seven-day period into the `customer_week` schema."""
    columns = [
        "window_start", "window_end", "customer_id", "tx_count", "total_amount",
        "avg_amount", "distinct_categories"
    ]
    if transactions.empty:
        return pd.DataFrame(columns=columns)
    work = transactions.copy()
    end = pd.Timestamp.now(tz="UTC").floor("D") + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=7)
    work["event_time"] = pd.to_datetime(work["event_time"], utc=True)
    work = work[(work["event_time"] >= start) & (work["event_time"] < end)]
    if work.empty:
        return pd.DataFrame(columns=columns)
    result = (
        work.groupby("customer_id", as_index=False)
        .agg(tx_count=("transaction_id", "count"),
             total_amount=("amount", "sum"),
             avg_amount=("amount", "mean"),
             distinct_categories=("category", "nunique"))
    )
    result["window_start"] = start
    result["window_end"] = end
    result["total_amount"] = result["total_amount"].round(2)
    result["avg_amount"] = result["avg_amount"].round(4)
    return result[columns].sort_values("total_amount", ascending=False).reset_index(drop=True)
