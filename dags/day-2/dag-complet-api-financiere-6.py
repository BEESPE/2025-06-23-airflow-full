from datetime import datetime, timedelta
import logging
import random
import requests
from typing import Dict

import numpy as np
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}&localization=fr"

@dag(
    dag_id="dag-complet-api-financiere-6",
    schedule="@once",
    start_date=datetime(2025, 6, 1),
    catchup=False,
)
def taskflow():
    @task(
        task_id="extract",
        retries=2,
    )
    def extract_bitcoin_price(task_instance):
        execution_date = task_instance.execution_date
        dates = []
        prices = []
        formatted_date = execution_date.strftime("%d-%m-%Y")
        #response = requests.get(API.format(formatted_date)).json()["market_data"]
        #initial_price = response["current_price"]["usd"]
        initial_price = 100000
        for n_day_before in range(0, 10):
            date = execution_date - timedelta(days=n_day_before)
            formatted_date = date.strftime("%d-%m-%Y")
            #response = requests.get(API.format(formatted_date)).json()["market_data"]
            random_variation = round(random.uniform(-1000, 1000), 2)
            dates.append(date)
            prices.append(initial_price + random_variation)
        return {"dates": dates, "prices": prices}

    @task(multiple_outputs=True)
    def process_data(extraction: Dict[str, float]) -> Dict[str, float]:
        logging.info(extraction)
        prices = extraction["prices"][::-1]
        deltas = np.diff(prices)
        ups = [delta for delta in deltas if delta > 0]
        downs =  [delta for delta in deltas if delta < 0]
        up_avg = sum(ups) / len(ups)
        down_avg = sum(downs) / len(downs)
        rsi = 100 - 100/(1+up_avg/down_avg)
        return {
            "dates": extraction["dates"],
            "prices": prices,
            "rsi": rsi,
        }

    #@task
    #def create_dataframe(data):
    #    df = pd.DataFrame({
    #        "date": data["dates"],
    #        "prices": data["prices"],
    #        "rsi": [data["rsi"]] * len(data["dates"]),
    #    })
    #    return df

    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="postgres_localhost")
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS bitcoin_data (
                date DATE PRIMARY KEY,
                price FLOAT,
                rsi FLOAT
            );
        """
        postgres_hook.run(create_table_sql)

    @task
    def insert_data(data):
        postgres_hook = PostgresHook(postgres_conn_id="postgres_localhost")
        rows = [(date, price, data["rsi"]) for date, price in zip(data["dates"], data["prices"])]
        postgres_hook.insert_rows(
            table="bitcoin_data",
            rows=rows,
            target_fields=["date", "price", "rsi"],
            replace=True,
            replace_index="date",
        )

        # insert_sql = ...

    @task
    def load_data_from_db():
        postgres_hook = PostgresHook(postgres_conn_id="postgres_localhost")
        df = pd.read_sql("SELECT * FROM bitcoin_data ORDER BY date", con=postgres_hook.get_conn())
        # df = postgres_hook.get_pandas_df("SELECT * FROM bitcoin_data ORDER BY date")
        return df

    #@task
    #def calculate_weekly_average(df):
    #    df["week"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%W")
    #    weekly_average = df.groupby("week")["prices"].mean().reset_index()
    #    return {
    #        "daily_data": df,
    #        "weekly_average": weekly_average,
    #    }
       

    #@task
    #def store_data(data: Dict[str, float]):
    #    logging.info(f"Store: Weekly averages {data['weekly_average'].head()}")

    #store_data(
    #    calculate_weekly_average(
    #        create_dataframe(
    #            process_data(
    #                extract_bitcoin_price()
    #            )
    #        )
    #    )
    #)

    create_table_task = create_table()
    extracted_data_task = process_data(extract_bitcoin_price())
    insert_data_task = insert_data(extracted_data_task)
    load_data_task = load_data_from_db()

    def branch_task(df):
        print(len(df))
        if len(df) > 100:
            return "calculate_weekly_average"
        else:
            return "not_enough_data"

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=branch_task,
        op_args=[load_data_task],
    )

    calculate_weekly_average = BashOperator(
        task_id="calculate_weekly_average",
        bash_command="echo 'calculate_weekly_average'",
    )

    not_enough_data = BashOperator(
        task_id="not_enough_data",
        bash_command="echo 'not_enough_data'"
    )

    create_table_task >> extracted_data_task >> insert_data_task >> load_data_task >> branching
    branching >> calculate_weekly_average
    branching >> not_enough_data

taskflow()