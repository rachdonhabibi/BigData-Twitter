from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_MASTER_URL = "spark://spark-master:7077"
SPARK_SRC_DIR = "/opt/bitnami/spark/src"
MONGO_CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="twitter_pipeline",
    description="Orchestration des jobs Spark de traitement de tweets",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["twitter", "bigdata"],
) as dag:

    env_exports = """
        export HOME=/tmp
        export JAVA_TOOL_OPTIONS="-Duser.home=/tmp"
    """

    clean_tweets = BashOperator(
        task_id="clean_tweets",
        bash_command=env_exports + f"""
        spark-submit \
          --master {SPARK_MASTER_URL} \
          --packages {MONGO_CONNECTOR} \
          {SPARK_SRC_DIR}/processing/clean_tweets_job.py
        """,
    )

    compute_kpis = BashOperator(
        task_id="compute_kpis",
        bash_command=env_exports + f"""
        spark-submit \
          --master {SPARK_MASTER_URL} \
          --packages {MONGO_CONNECTOR} \
          {SPARK_SRC_DIR}/processing/analytics_tweets_job.py
        """,
    )

    build_social_graph = BashOperator(
        task_id="build_social_graph",
        bash_command=env_exports + f"""
        spark-submit \
          --master {SPARK_MASTER_URL} \
          --packages {MONGO_CONNECTOR} \
          {SPARK_SRC_DIR}/processing/social_graph_job.py
        """,
    )

    clean_tweets >> compute_kpis >> build_social_graph
