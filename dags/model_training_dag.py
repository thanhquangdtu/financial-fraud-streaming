"""DAG 2 - model training with a quality gate.

The candidate model is trained into data/models/candidate/ and only replaces the live model
(data/models/fraud_gbt.json, read by Spark) if the untouched TEST split reaches F1 >= 0.80 and PR-AUC >= 0.85.
Spark loads the model at startup: after a promotion run `docker compose restart spark` to use it.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT = Path(os.environ.get("PROJECT_DIR", "/opt/airflow/project"))
CANDIDATE_MODEL = PROJECT / "data/models/candidate/fraud_gbt.json"
CANDIDATE_REPORT = PROJECT / "data/models/candidate/ml_report.json"


def quality_gate_and_promote():
    from financial_streaming.ml.gate import gate_and_promote

    verdict = gate_and_promote(
        CANDIDATE_MODEL, CANDIDATE_REPORT,
        PROJECT / "data/models/fraud_gbt.json", PROJECT / "data/reports/ml_report.json",
    )
    print("model promoted:", verdict)
    return verdict


with DAG(
    dag_id="model_training",
    description="Week 7: train candidate -> quality gate -> promote",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fraud", "week7"],
) as dag:
    check_data_ready = BashOperator(
        task_id="check_data_ready",
        bash_command=f"test -s {PROJECT}/data/processed/train.csv && test -s {PROJECT}/data/processed/validation.csv "
                     f"&& test -s {PROJECT}/data/processed/test.csv",
    )
    train_candidate = BashOperator(
        task_id="train_candidate",
        bash_command=(f"cd {PROJECT} && python -m financial_streaming.ml.train "
                      f"--model-out {CANDIDATE_MODEL} --report-out {CANDIDATE_REPORT}"),
        env={**os.environ, "PYTHONPATH": f"{PROJECT}/src"},
        execution_timeout=None,
    )
    gate = PythonOperator(task_id="quality_gate_and_promote", python_callable=quality_gate_and_promote)

    check_data_ready >> train_candidate >> gate
