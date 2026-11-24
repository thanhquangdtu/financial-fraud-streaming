"""Train the fraud model (Week 5).

    python -m financial_streaming.ml.train

- fit on data/processed/train.csv only
- choose the decision threshold on validation.csv (max F1)
- report precision / recall / F1 / PR-AUC on validation AND on the untouched test.csv
- write data/models/fraud_gbt.json (used by Spark) and data/reports/ml_report.json
`fraud_label` is the target only, never a feature.
"""
from __future__ import annotations

import argparse
import time

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import average_precision_score, f1_score, precision_recall_curve, precision_score, recall_score, roc_auc_score

from ..preprocessing.common import ROOT, write_json
from .export import export_gbt, predict_proba_json
from .features import build_feature_spec, read_split, transform


def metrics_at(y, p, threshold: float) -> dict:
    pred = (p >= threshold).astype(int)
    return {
        "threshold": round(float(threshold), 4),
        "precision": round(float(precision_score(y, pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y, pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y, pred, zero_division=0)), 4),
        "flagged": int(pred.sum()),
        "actual_fraud": int(y.sum()),
        "pr_auc": round(float(average_precision_score(y, p)), 4),
        "roc_auc": round(float(roc_auc_score(y, p)), 4),
        "rows": int(len(y)),
    }


def best_f1_threshold(y, p) -> float:
    prec, rec, thr = precision_recall_curve(y, p)
    f1 = 2 * prec[:-1] * rec[:-1] / np.clip(prec[:-1] + rec[:-1], 1e-12, None)
    return float(thr[int(np.argmax(f1))])


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--trees", type=int, default=100)
    ap.add_argument("--depth", type=int, default=4)
    ap.add_argument("--learning-rate", type=float, default=0.1)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--model-out", default=str(ROOT / "data/models/fraud_gbt.json"),
                    help="where to write the exported model (Airflow writes a candidate, then promotes it)")
    ap.add_argument("--report-out", default=str(ROOT / "data/reports/ml_report.json"))
    a = ap.parse_args(argv)

    train = read_split(ROOT / "data/processed/train.csv")
    val = read_split(ROOT / "data/processed/validation.csv")
    test = read_split(ROOT / "data/processed/test.csv")
    assert train["step"].astype(int).max() < val["step"].astype(int).min() <= val["step"].astype(int).max() < test["step"].astype(int).min()

    spec = build_feature_spec(train)
    Xtr, Xva, Xte = (transform(d, spec) for d in (train, val, test))
    ytr, yva, yte = (d["fraud_label"].to_numpy() for d in (train, val, test))

    t0 = time.time()
    model = GradientBoostingClassifier(n_estimators=a.trees, max_depth=a.depth, learning_rate=a.learning_rate,
                                       subsample=0.8, random_state=a.seed)
    model.fit(Xtr, ytr)
    train_seconds = round(time.time() - t0, 1)

    pva, pte = model.predict_proba(Xva)[:, 1], model.predict_proba(Xte)[:, 1]
    threshold = best_f1_threshold(yva, pva)

    # Baseline to show the model adds value: flag the top-k% highest amounts (k = train fraud rate).
    cut = np.quantile(train["amount"], 1 - ytr.mean())
    base_val = metrics_at(yva, val["amount"].to_numpy(), cut)
    base_test = metrics_at(yte, test["amount"].to_numpy(), cut)

    report = {
        "model": "GradientBoostingClassifier",
        "params": {"trees": a.trees, "depth": a.depth, "learning_rate": a.learning_rate, "seed": a.seed, "subsample": 0.8},
        "features": len(spec),
        "feature_note": "amount, log(amount), one-hot of category/age_group/gender/merchant_id (vocabulary from train only); fraud_label is never a feature",
        "train": {"rows": int(len(ytr)), "fraud": int(ytr.sum()), "seconds": train_seconds},
        "threshold_chosen_on": "validation (max F1)",
        "validation": metrics_at(yva, pva, threshold),
        "test": metrics_at(yte, pte, threshold),
        "baseline_amount_only": {"validation": base_val, "test": base_test,
                                 "rule": f"flag amount >= {cut:.2f} (top {ytr.mean() * 100:.2f}% of train amounts)"},
    }

    model_json = export_gbt(model, spec, threshold, meta={"validation": report["validation"], "test": report["test"]})
    # The exported JSON must reproduce scikit-learn exactly, otherwise Spark would score differently.
    sample = Xte[:2000]
    diff = float(np.abs(predict_proba_json(model_json, sample) - pte[:2000]).max())
    report["export_max_abs_diff_vs_sklearn"] = diff
    assert diff < 1e-6, f"exported model differs from sklearn by {diff}"

    write_json(a.model_out, model_json)
    write_json(a.report_out, report)
    for name in ("validation", "test"):
        print(name, report[name])
    print("baseline test", base_test)
    print("export diff", diff, "| train seconds", train_seconds)


if __name__ == "__main__":
    main()
