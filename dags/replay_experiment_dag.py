"""DAG 3 - replay experiment: send a slice of the TEST split through Kafka -> Spark -> PostgreSQL and verify it.

Uses its own checkpoint/control files so it never disturbs the dashboard's replay position.
Rows of test.csv (steps 153+) are newer than anything already streamed, so Spark's watermark does not drop them.
Needs the stack running: docker compose up -d
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT = Path(os.environ.get("PROJECT_DIR", "/opt/airflow/project"))
sys.path.insert(0, str(PROJECT / "src"))

TEST_CSV = PROJECT / "data/processed/test.csv"


def check_services():
    from financial_streaming.experiment import check_services as check

    check()


def verify_replayed(**context):
    from financial_streaming.experiment import read_first_ids, wait_until_scored

    ids = read_first_ids(TEST_CSV, int(context["params"]["events"]))
    result = wait_until_scored(ids, timeout_s=float(context["params"]["timeout_s"]))
    print("all replayed events are in PostgreSQL with a fraud_score:", result)
    return result


with DAG(
    dag_id="replay_experiment",
    description="Week 7: check services -> replay test slice -> verify rows in PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"events": Param(300, type="integer", minimum=1), "rate": Param(100, type="integer", minimum=1),
            "timeout_s": Param(180, type="integer", minimum=10)},
    tags=["fraud", "week7"],
) as dag:
    services = PythonOperator(task_id="check_kafka_postgres", python_callable=check_services)
    replay = BashOperator(
        task_id="replay_test_slice",
        bash_command=(f"cd {PROJECT} && python -m financial_streaming.producer.cli run "
                      f"--input {TEST_CSV} --from-start --limit {{{{ params.events }}}} --rate {{{{ params.rate }}}}"),
        env={**os.environ, "PYTHONPATH": f"{PROJECT}/src",
             "REPLAY_CHECKPOINT_FILE": str(PROJECT / "data/replay/airflow_checkpoint.json"),
             "REPLAY_CONTROL_FILE": str(PROJECT / "data/replay/airflow_control")},
    )
    verify = PythonOperator(task_id="verify_in_postgres", python_callable=verify_replayed)

    services >> replay >> verify
