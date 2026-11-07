"""dbt: staging views -> SCD2 snapshots -> marts -> tests.

Order matters: snapshots SELECT from the staging views, and the marts read both
the snapshots and the staging views, so staging must be built first.
"""
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT = "/home/airflow/dbt-venv/bin/dbt"
PROJECT_DIR = "/opt/airflow/banking_dbt"
PROFILES_DIR = "/opt/airflow/dbt_profiles"


def dbt(cmd: str) -> str:
    return f"cd {PROJECT_DIR} && {DBT} {cmd} --profiles-dir {PROFILES_DIR}"


with DAG(
    dag_id="SCD2_snapshots",
    description="Run dbt staging, snapshots (SCD2), marts and tests",
    schedule=os.getenv("DBT_SCHEDULE", "@daily"),
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "depends_on_past": False, "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["dbt", "snapshots"],
) as dag:
    dbt_staging = BashOperator(task_id="dbt_run_staging", bash_command=dbt("run --select staging"))
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=dbt("snapshot"))
    dbt_marts = BashOperator(task_id="dbt_run_marts", bash_command=dbt("run --select marts"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=dbt("test"))

    dbt_staging >> dbt_snapshot >> dbt_marts >> dbt_test
