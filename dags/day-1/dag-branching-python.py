import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import timedelta
from pendulum import datetime
import random


def process_data():
    # Lire les données depuis le fichier CSV téléchargé
    df = pd.read_csv('/tmp/flowerdataset.csv')

    # Calcul de la somme sepal_length + sepal_width
    df["somme"] = df["sepal_length"] + df["sepal_width"]
    print(df.head())

def branch_by_option():
    if random.randint(0, 1) == 0:
        return 'bored'
    return 'fun'

def print_message(message):
    print(message)

# Définir les arguments par défaut du DAG
default_args = {
    'owner': 'bsp',
    'start_date': datetime(2024, 3, 25),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

# Instancier le DAG
dag = DAG(
    'dag-branching-python',
    default_args=default_args,
    schedule_interval='@daily',
)

# Tâche pour télécharger les données
download_data_task = BashOperator(
    task_id='download_data',
    bash_command='curl -o /tmp/flowerdataset.csv https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv',
    dag=dag
)

# Tâche pour traiter les données et les stocker dans la base de données
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag
)

branching_task = BranchPythonOperator(
    task_id='branching',
    python_callable=branch_by_option,
    dag=dag,
)

bored_task = PythonOperator(
    task_id='bored',
    python_callable=print_message,
    op_kwargs={'message': 'Malheureusement il faut travailler'},
    dag=dag,
)

fun_task = PythonOperator(
    task_id='fun',
    python_callable=print_message,
    op_kwargs={'message': 'Belle journee pour ramasser des fleurs'},
    dag=dag,
)

download_data_task >> process_data_task >> branching_task
branching_task >> [bored_task, fun_task]