import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from pendulum import datetime


def process_data():
    # Lire les données depuis le fichier CSV téléchargé
    df = pd.read_csv('/tmp/flowerdataset.csv')

    # Calcul de la somme sepal_length + sepal_width
    df["volume"] = df["sepal_length"] * df["sepal_width"]
    print(df.head())


# Définir les arguments par défaut du DAG
default_args = {
    'owner': 'bsp',
    'start_date': datetime(2025, 3, 21),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

# Instancier le DAG
dag = DAG(
    'dag-get-ext-data',
    default_args=default_args,
    schedule_interval='@daily',
)

# Tâche pour télécharger les données
download_data_task = BashOperator(
    task_id='download_data',
    bash_command='curl -o /tmp/flowerdataset.csv https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv',
    dag=dag
)

# Tâche pour traiter les données
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag
)

# Définir les dépendances entre les tâches
download_data_task >> process_data_task
