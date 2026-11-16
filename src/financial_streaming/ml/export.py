"""Export a scikit-learn GradientBoostingClassifier to plain JSON and score with it (pure numpy).

The same JSON is turned into Spark column expressions in streaming/scoring.py, so the streaming job
needs no ML library. `predict_proba_json` is the reference used by tests to prove the export is exact.
"""
from __future__ import annotations

import numpy as np


def export_gbt(model, spec: list[dict], threshold: float, meta: dict | None = None) -> dict:
    lr = float(model.learning_rate)
    trees = []
    for est in model.estimators_[:, 0]:
        t = est.tree_
        trees.append({
            "left": t.children_left.tolist(),
            "right": t.children_right.tolist(),
            "feature": t.feature.tolist(),
            "threshold": t.threshold.tolist(),
            "value": t.value[:, 0, 0].tolist(),
        })
    # Initial raw score, taken from the model itself so it does not depend on scikit-learn internals.
    x0 = np.zeros((1, len(spec)), dtype=np.float32)
    raw0 = float(model.decision_function(x0)[0])
    tree_sum0 = sum(_leaf(tr, x0[0]) for tr in trees)
    return {
        "type": "gradient_boosting_binary",
        "learning_rate": lr,
        "init_raw": raw0 - lr * tree_sum0,
        "threshold": float(threshold),
        "features": spec,
        "trees": trees,
        "meta": meta or {},
    }


def _leaf(tree: dict, row) -> float:
    node = 0
    while tree["left"][node] != -1:
        node = tree["left"][node] if row[tree["feature"][node]] <= tree["threshold"][node] else tree["right"][node]
    return tree["value"][node]


def predict_proba_json(model_json: dict, X: np.ndarray) -> np.ndarray:
    lr = model_json["learning_rate"]
    raw = np.full(len(X), model_json["init_raw"], dtype=np.float64)
    for i, row in enumerate(X):
        raw[i] += lr * sum(_leaf(tr, row) for tr in model_json["trees"])
    return 1.0 / (1.0 + np.exp(-raw))
