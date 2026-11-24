"""DAG 1 - data preparation: verify raw data, clean, split, generate synthetic data, validate quality.

Order matters: if `validate_quality` fails, nothing downstream (model training) should run.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = os.environ.get("PROJECT_DIR", "/opt/airflow/project")


def step(task_id: str, module: str, args: str = "") -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT} && python -m financial_streaming.preprocessing.{module} {args}",
        env={**os.environ, "PYTHONPATH": f"{PROJECT}/src"},
        retries=1,
    )


with DAG(
    dag_id="data_preparation",
    description="Week 7: checksum -> inspect -> clean -> synthetic -> split -> validate",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # run on demand; set e.g. "@weekly" to automate
    catchup=False,
    tags=["fraud", "week7"],
) as dag:
    verify_checksum = step("verify_checksum", "checksum", "--verify")
    inspect_raw = step("inspect_raw", "inspect")
    clean_banksim = step("clean_banksim", "clean")
    gen_synthetic = step("gen_synthetic", "synthetic")
    split_dataset = step("split_dataset", "split")
    validate_quality = step("validate_quality", "validate")

    verify_checksum >> inspect_raw >> clean_banksim
    clean_banksim >> [split_dataset, gen_synthetic] >> validate_quality
