"""Orchestrates the Bronze -> Silver -> data quality -> Gold -> ClickHouse
pipeline that used to be three Databricks notebooks run by hand.

Each task shells out to the dedicated pipeline virtualenv baked into the
Airflow image (see docker/airflow/Dockerfile) rather than importing
insurance_pipeline into the Airflow process itself, so PySpark/Great
Expectations dependencies never collide with Airflow's own.
"""

from __future__ import annotations

import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

ENV = {
    "DATA_ROOT": "/opt/airflow/data/samples",
    "SCHEMA_ROOT": "/opt/airflow/data/schemas",
    "WAREHOUSE_DIR": "/opt/airflow/warehouse",
    "CLICKHOUSE_HOST": "clickhouse",
    "CLICKHOUSE_PORT": "8123",
    "CLICKHOUSE_USER": "default",
    "CLICKHOUSE_PASSWORD": "",
    "CLICKHOUSE_DATABASE": "insurance",
}


def _step_command(step: str) -> str:
    return f"$PIPELINE_PYTHON -m insurance_pipeline.run {step}"


with DAG(
    dag_id="insurance_pipeline_dag",
    description="Bronze -> Silver -> data quality -> Gold -> ClickHouse",
    schedule="@daily",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["insurance", "medallion"],
) as dag:
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=_step_command("bronze"),
        env=ENV,
        append_env=True,
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=_step_command("silver"),
        env=ENV,
        append_env=True,
    )

    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=_step_command("quality"),
        env=ENV,
        append_env=True,
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=_step_command("gold"),
        env=ENV,
        append_env=True,
    )

    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        bash_command=_step_command("clickhouse"),
        env=ENV,
        append_env=True,
    )

    ingest_bronze >> transform_silver >> data_quality_checks >> build_gold >> load_clickhouse
