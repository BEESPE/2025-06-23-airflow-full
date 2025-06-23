import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import timedelta
from pendulum import datetime

def process_data():
    # Lire les données depuis le fichier CSV téléchargé
    df = pd.read_csv('/tmp/flowerdataset.csv')

    # Calcul de la somme sepal_length + sepal_width
    df["somme"] = df["sepal_length"] + df["sepal_width"]
    print(df.head())
    
    largest_sepal_flower = df.sort_values(by="petal_length", ascending=False).iloc[0]

    return {
        "somme": largest_sepal_flower["somme"],
        "sepal_length": largest_sepal_flower["sepal_length"],
        "sepal_width": largest_sepal_flower["sepal_width"],
    }

# Définir les arguments par défaut du DAG
default_args = {
    'owner': 'bsp',
    'start_date': datetime(2024, 3, 25),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

# Instancier le DAG
dag = DAG('dag-get-ext-data-store', default_args=default_args,
          schedule_interval='@daily')

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
    dag=dag,
)

create_task = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS flower (
                somme FLOAT,
                sepal_length FLOAT,
                sepal_width FLOAT
            )
        """,
        dag=dag,
    )

insert_task = PostgresOperator(
    task_id='insert_into_postgres',
    postgres_conn_id='postgres_localhost',
    sql="""
    INSERT INTO flower (somme, sepal_length, sepal_width)
    VALUES (
        {{ task_instance.xcom_pull(task_ids='process_data')['somme'] }},
        {{ task_instance.xcom_pull(task_ids='process_data')['sepal_length'] }},
        {{ task_instance.xcom_pull(task_ids='process_data')['sepal_width'] }}
    )
    """,
)

# Définir les dépendances entre les tâches
download_data_task >> process_data_task >> create_task >> insert_task
