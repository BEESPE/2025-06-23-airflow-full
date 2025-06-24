from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "bsp",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

def fetch_and_process(**kwargs):
    target_dag_id = kwargs["params"].get("target_dag_id")

    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT * from dag_runs")
    rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=["dt", "dag_id"])
    
    filtered_df = df[df["dag_id"] == target_dag_id]

    print(filtered_df)


with DAG(
    dag_id="dag-manip-param",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule_interval="0 0 * * *",
    params={
        "target_dag_id": "dag-with-postgres-operator"
    }
) as dag:
    create_table_task = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            create table if not exists dag_runs (
            dt date,
            dag_id character varying,
            primary key (dt, dag_id)
            )
        """
    )

    delete_data_task = PostgresOperator(
        task_id="delete_data_from_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}';
        """
    )

    insert_data_task = PostgresOperator(
        task_id="insert_into_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )

    fetch_and_process_task = PythonOperator(
        task_id="fetch_and_process_data",
        python_callable=fetch_and_process,
    )

    create_table_task >> delete_data_task >> insert_data_task >> fetch_and_process_task