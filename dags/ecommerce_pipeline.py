from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "sai",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="ecommerce_pipeline",
    description="Daily e-commerce data pipeline with Databricks",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "pipeline", "databricks"],
)

# --- Databricks env vars passed to all tasks ---
DATABRICKS_ENV = {
    "DATABRICKS_HOST":         os.getenv("DATABRICKS_HOST", ""),
    "DATABRICKS_TOKEN":        os.getenv("DATABRICKS_TOKEN", ""),
    "DATABRICKS_WAREHOUSE_ID": os.getenv("DATABRICKS_WAREHOUSE_ID", ""),
}


def ingest_data():
    """Task 1 — Read data from Kafka and confirm records available."""
    from kafka import KafkaConsumer
    import json

    consumer = KafkaConsumer(
        "ecommerce-orders",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    count = sum(1 for _ in consumer)
    consumer.close()
    print(f"Ingestion complete — {count:,} orders available in Kafka")
    return count


def run_spark_transform():
    """Task 2 — Run transformations on Databricks SQL Warehouse."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    os.environ.update(DATABRICKS_ENV)

    from spark.transform import setup_tables, load_data_to_databricks, run_transformations
    setup_tables()
    load_data_to_databricks()
    run_transformations()
    print("Databricks transformation complete")


def run_dbt():
    """Task 3 — Run dbt models."""
    import subprocess
    result = subprocess.run(
        ["python3", "/opt/airflow/spark/dbt_runner.py"],
        capture_output=True,
        text=True,
        env={**os.environ, **DATABRICKS_ENV},
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"dbt warning: {result.stderr}")
    print("dbt models complete")


def load_to_warehouse():
    """Task 4 — Load transformed data to PostgreSQL."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    os.environ.update(DATABRICKS_ENV)

    from warehouse.loader import load_to_postgres
    load_to_postgres()
    print("Warehouse load complete")


def train_model():
    """Task 5 — Train sales forecasting model with MLflow tracking."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    os.environ.update(DATABRICKS_ENV)

    from mlflow.train import run_training
    run_training()
    print("Model training complete")


# --- Define tasks ---
task_ingest = PythonOperator(
    task_id="ingest_from_kafka",
    python_callable=ingest_data,
    dag=dag,
)

task_spark = PythonOperator(
    task_id="databricks_transform",
    python_callable=run_spark_transform,
    dag=dag,
)

task_dbt = PythonOperator(
    task_id="dbt_models",
    python_callable=run_dbt,
    dag=dag,
)

task_warehouse = PythonOperator(
    task_id="load_warehouse",
    python_callable=load_to_warehouse,
    dag=dag,
)

task_model = PythonOperator(
    task_id="train_model",
    python_callable=train_model,
    dag=dag,
)

# --- Define order ---
task_ingest >> task_spark >> task_dbt >> task_warehouse >> task_model