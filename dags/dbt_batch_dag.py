from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dbt_batch_models",
    default_args=default_args,
    description="Executa os modelos do dbt em lote na camada Gold",
    schedule_interval="@daily",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["dbt", "analytics", "gold"],
) as dag:
    # Command to run seeds
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed --project-dir /opt/airflow/analytics --profiles-dir /opt/airflow/analytics",
    )

    # Command to run snapshots
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="dbt snapshot --project-dir /opt/airflow/analytics --profiles-dir /opt/airflow/analytics",
    )

    # Command to run models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /opt/airflow/analytics --profiles-dir /opt/airflow/analytics",
    )

    # Command to test data quality (tests)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/airflow/analytics --profiles-dir /opt/airflow/analytics",
    )

    # Execution flow
    dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test
