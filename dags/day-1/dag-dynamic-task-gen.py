from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import random

def generate_tasks(task_id_prefix, num_tasks):
    """Fonction pour la génération dynamique des tâches.
    """
    tasks = []
    for i in range(num_tasks):
        task_id = f"{task_id_prefix}_{i}"
        bash_command = f"echo 'Executing task {task_id}'"
        task = BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            dag=dag,
        )
        tasks.append(task)
    return tasks

default_args = {
    'owner': 'bsp',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag-dynamic-task-gen',
    default_args=default_args,
    description='DAG de démo Dynamic Task Generation',
    schedule_interval=timedelta(days=1),
)

generated_tasks = generate_tasks("dynamic_task", 10)

final_task = BashOperator(
    task_id='final_task',
    bash_command='echo "All tasks completed"',
    dag=dag,
)

for task in generated_tasks:
    task >> final_task
