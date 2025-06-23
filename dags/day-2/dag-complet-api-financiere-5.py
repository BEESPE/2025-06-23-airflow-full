from datetime import datetime, timedelta
import logging
import random
import requests
from typing import Dict

import numpy as np
import pandas as pd

from airflow.decorators import dag, task

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}&localization=fr"

@dag(
    dag_id="dag-complet-api-financiere-5",
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

    @task
    def create_dataframe(data):
        df = pd.DataFrame({
            "date": data["dates"],
            "prices": data["prices"],
            "rsi": [data["rsi"]] * len(data["dates"]),
        })
        return df

    @task
    def calculate_weekly_average(df):
        df["week"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%W")
        weekly_average = df.groupby("week")["prices"].mean().reset_index()
        return {
            "daily_data": df,
            "weekly_average": weekly_average,
        }
       

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: Weekly averages {data['weekly_average'].head()}")

    store_data(
        calculate_weekly_average(
            create_dataframe(
                process_data(
                    extract_bitcoin_price()
                )
            )
        )
    )


taskflow()