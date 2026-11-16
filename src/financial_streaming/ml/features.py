"""Feature definition shared by training and streaming scoring (Week 5).

Rules (ai/AGENTS.md): the ground-truth label (`fraud_label`) is NEVER a feature, and every vocabulary
is learned from the TRAIN split only (no leakage from validation/test).
Only fields that exist on a single event at ingestion time are used, so Spark can compute the very same
features from the Kafka message without any state.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

CATEGORICAL = ["category", "age_group", "gender", "merchant_id"]
FORBIDDEN = {"fraud_label", "fraud", "scenario_anomaly"}


def read_split(path) -> pd.DataFrame:
    """Read a processed split with string identifiers and a numeric amount."""
    df = pd.read_csv(path, dtype={c: str for c in CATEGORICAL + ["customer_id", "transaction_id"]}, keep_default_na=False)
    df["amount"] = df["amount"].astype(float)
    df["fraud_label"] = df["fraud_label"].astype(int)
    return df


def build_feature_spec(train: pd.DataFrame) -> list[dict]:
    """Feature list; the one-hot vocabulary comes from `train` only."""
    spec = [{"name": "amount", "kind": "amount"}, {"name": "log_amount", "kind": "log_amount"}]
    for col in CATEGORICAL:
        for value in sorted(train[col].astype(str).unique()):
            spec.append({"name": f"{col}={value}", "kind": "onehot", "column": col, "value": value})
    assert not any(f["name"].split("=")[0] in FORBIDDEN for f in spec), "label used as feature"
    return spec


def transform(df: pd.DataFrame, spec: list[dict]) -> np.ndarray:
    """Feature matrix (float32, like scikit-learn trees use internally)."""
    cols = []
    for f in spec:
        if f["kind"] == "amount":
            cols.append(df["amount"].to_numpy(dtype=np.float64))
        elif f["kind"] == "log_amount":
            cols.append(np.log1p(df["amount"].to_numpy(dtype=np.float64)))
        else:
            cols.append((df[f["column"]].astype(str) == f["value"]).to_numpy(dtype=np.float64))
    return np.column_stack(cols).astype(np.float32)
