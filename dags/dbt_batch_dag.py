import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_batch_models',
    default_args=default_args,
    description='Executa os modelos do dbt em lote na camada Gold',
    schedule_interval='@daily',
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['dbt', 'analytics', 'gold'],
) as dag:

    # Comando para rodar os seeds
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='dbt seed --project-dir /opt/airflow/analytics --profiles-dir /opt/airflow/analytics',
    )

    # Comando para rodar os snapshots
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='dbt snapshot --project-dir /opt/airflow/analytics --profiles-dir /opt/airflow/analytics',
    )

    # Comando para rodar os modelos
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir /opt/airflow/analytics --profiles-dir /opt/airflow/analytics',
    )

    # Comando para testar a qualidade dos dados (testes)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir /opt/airflow/analytics --profiles-dir /opt/airflow/analytics',
    )

    # Fluxo de execução
    dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test
