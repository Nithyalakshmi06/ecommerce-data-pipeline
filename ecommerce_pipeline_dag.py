from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "nithya",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Transforms raw e-commerce events into cleaned Parquet",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 7, 16),
    catchup=False,
    tags=["ecommerce", "spark", "learning-project"],
) as dag:

    check_infra = BashOperator(
        task_id="check_infra",
        bash_command=(
            "echo 'Checking MinIO...' && "
            "curl -sf http://localhost:9000/minio/health/live || "
            "(echo 'MinIO is not reachable. Run: docker compose up -d' && exit 1)"
        ),
    )

    run_spark_transform = BashOperator(
        task_id="run_spark_transform",
        bash_command=(
            "cd ~/ecomerce-pipeline && "
            "source venv/bin/activate && "
            "python spark_jobs/transform.py"
        ),
    )

    check_infra >> run_spark_transform
