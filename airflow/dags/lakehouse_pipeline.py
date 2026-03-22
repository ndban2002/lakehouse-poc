from __future__ import annotations

import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion.postgres_to_minio import run as ingest_run


def ingest_task(**context):
    run_date = context["ds"]
    destination = ingest_run(run_date=run_date)
    context["ti"].xcom_push(key="raw_path", value=destination)


def spark_transform_task(**context):
    run_date = context["ds"]
    command = [
        "python",
        "/opt/airflow/spark/jobs/bronze_to_silver.py",
        "--run-date",
        run_date,
    ]
    subprocess.run(command, check=True)


with DAG(
    dag_id="lakehouse_banking_transactions_to_iceberg",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["lakehouse", "poc"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_banking_transactions_to_minio",
        python_callable=ingest_task,
    )

    transform = PythonOperator(
        task_id="transform_banking_transactions_to_iceberg",
        python_callable=spark_transform_task,
    )

    ingest >> transform
