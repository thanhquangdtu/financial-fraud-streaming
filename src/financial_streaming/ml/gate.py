"""Model quality gate and safe promotion (Week 7).

Airflow trains a CANDIDATE model, checks it here, and only then replaces the live model that Spark reads.
A bad retrain therefore can never overwrite a good model.
"""
from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

MIN_TEST_F1 = 0.80
MIN_TEST_PR_AUC = 0.85


def check_quality(report: dict, min_f1: float = MIN_TEST_F1, min_pr_auc: float = MIN_TEST_PR_AUC) -> dict:
    """Return a verdict dict; `passed` is True only if the untouched TEST split meets both thresholds."""
    test = report["test"]
    problems = []
    if test["f1"] < min_f1:
        problems.append(f"test F1 {test['f1']} < {min_f1}")
    if test["pr_auc"] < min_pr_auc:
        problems.append(f"test PR-AUC {test['pr_auc']} < {min_pr_auc}")
    if report.get("export_max_abs_diff_vs_sklearn", 0.0) >= 1e-6:
        problems.append("exported model does not match scikit-learn")
    return {"passed": not problems, "problems": problems, "test_f1": test["f1"], "test_pr_auc": test["pr_auc"]}


def promote(candidate_model: Path, candidate_report: Path, live_model: Path, live_report: Path) -> None:
    """Replace the live model/report with the candidate (copy to a temp file, then atomic replace)."""
    for src, dst in ((candidate_model, live_model), (candidate_report, live_report)):
        dst = Path(dst)
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_suffix(dst.suffix + ".tmp")
        shutil.copyfile(src, tmp)
        os.replace(tmp, dst)


def gate_and_promote(candidate_model: Path, candidate_report: Path, live_model: Path, live_report: Path,
                     min_f1: float = MIN_TEST_F1, min_pr_auc: float = MIN_TEST_PR_AUC) -> dict:
    """Raise ValueError (so the Airflow task fails) unless the candidate passes the gate; promote it if it does."""
    report = json.loads(Path(candidate_report).read_text(encoding="utf-8"))
    verdict = check_quality(report, min_f1, min_pr_auc)
    if not verdict["passed"]:
        raise ValueError("model rejected, live model kept: " + "; ".join(verdict["problems"]))
    promote(candidate_model, candidate_report, live_model, live_report)
    return verdict
