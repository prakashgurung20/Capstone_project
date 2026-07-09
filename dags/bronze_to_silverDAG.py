from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SILVER_JOB = "/spark/jobs/silver_streaming.py"
BRONZE_DIR = "/data/bronze/payments"
SILVER_DIR = "/data/silver/payments"

default_args = {
    "owner":            "capstone_project",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="payments_bronze_to_silver_hourly",
    default_args=default_args,
    description="Hourly Bronze to Silver ETL",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["payments", "spark", "medallion"],
) as dag:

    run_spark_job = BashOperator(
        task_id="spark_submit_refine_payments",
        bash_command=(
            "docker exec spark-etl "
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            f"{SILVER_JOB} "
            f"--bronze-path {BRONZE_DIR} "
            f"--silver-path {SILVER_DIR} "
            "--year {{ execution_date.year }} "
            "--month {{ execution_date.month }} "
            "--day {{ execution_date.day }}"
        ),
        sla=timedelta(minutes=30),
    )

    check_output = BashOperator(
        task_id="verify_silver_output",
        bash_command=(
            "docker exec spark-etl ls "
            f"{SILVER_DIR}"
            "/year={{ execution_date.year }}"
            "/month={{ execution_date.month }}"
            "/day={{ execution_date.day }}"
        )
    )

    run_spark_job >> check_output