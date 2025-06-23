from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'bsp',
    'start_date': datetime(2024, 3, 25),
}

dag = DAG(
    dag_id='dag-timeout',
    default_args=default_args,
    schedule_interval='@daily',
)

task1 = BashOperator(
    task_id='task1',
    bash_command='sleep 120',
    execution_timeout=timedelta(minutes=1),
    dag=dag,
)
