import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/bitnami/airflow"  # racine montée dans le conteneur
# src est monté sur /opt/bitnami/airflow/src


def run_kafka_producer():
    # Équivaut à: python -m src.ingestion.producer
    subprocess.run(
        ["python", "-m", "src.ingestion.producer"],
        cwd=PROJECT_ROOT,
        check=True,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="twitter_ingestion",
    description="Ingestion batch des tweets vers Kafka (replay depuis CSV)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",  # par ex. toutes les heures, ou '@once' si tu le lances à la main
    catchup=False,
    tags=["twitter", "ingestion"],
) as dag:

    ingest_task = PythonOperator(
        task_id="run_kafka_producer",
        python_callable=run_kafka_producer,
    )
