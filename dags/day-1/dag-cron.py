from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'bsp',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    default_args=default_args,
    dag_id="dag-cron",
    start_date=datetime(2024, 3, 18),
    schedule_interval='0 10 * * Tue-Fri'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo un simple affichage"
    )
    task1