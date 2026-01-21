import os
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# Dossier src monté dans le conteneur Airflow
PROJECT_SRC_DIR = "/opt/bitnami/airflow/src"


def run_clean_tweets():
    script_path = os.path.join(PROJECT_SRC_DIR, "processing", "clean_tweets_job.py")
    subprocess.run(["python", script_path], check=True)


def run_analytics_kpis():
    script_path = os.path.join(PROJECT_SRC_DIR, "processing", "analytics_tweets_job.py")
    subprocess.run(["python", script_path], check=True)


def run_social_graph():
    script_path = os.path.join(PROJECT_SRC_DIR, "processing", "social_graph_job.py")
    subprocess.run(["python", script_path], check=True)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="twitter_pipeline",
    description="Pipeline batch: nettoyage, KPIs, graphe social pour les tweets Ukraine",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",  # ou None si tu veux le déclencher à la main
    catchup=False,
    tags=["twitter", "bigdata"],
) as dag:

    clean_tweets = PythonOperator(
        task_id="clean_tweets",
        python_callable=run_clean_tweets,
    )

    compute_kpis = PythonOperator(
        task_id="compute_kpis",
        python_callable=run_analytics_kpis,
    )

    build_social_graph = PythonOperator(
        task_id="build_social_graph",
        python_callable=run_social_graph,
    )

    clean_tweets >> compute_kpis >> build_social_graph
