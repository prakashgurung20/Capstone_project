from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

JOB_FILE = "/opt/airflow/jobs/bronze_to_silver.py"
BRONZE_DIR = "/opt/airflow/data/bronze/payments"
SILVER_DIR = "/opt/airflow/data/silver/payments"

default_args = {
    "owner": "prakash_gurung",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="payments_bronze_to_silver_hourly",
    default_args=default_args,
    description="Refines payment data from Bronze to Silver layer",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["payments", "spark", "medallion"],
) as dag:

    run_spark_job = BashOperator(
        task_id="spark_submit_refine_payments",
        bash_command=(
            f"spark-submit --master local[*] "
            f"{JOB_FILE} "
            f"--bronze-path {BRONZE_DIR} "
            f"--silver-path {SILVER_DIR} "
            f"--processing-date '{{{{ ds }}}}'"
        )
    )

    check_output = BashOperator(
        task_id="verify_silver_output",
        bash_command=f"ls -l {SILVER_DIR}/event_date={{{{ ds }}}}"
    )

    run_spark_job >> check_output