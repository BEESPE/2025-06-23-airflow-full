from datetime import datetime, timedelta
import logging
import numpy as np
import random
import requests
from typing import Dict

from airflow.decorators import dag, task

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}&localization=fr"

@dag(
    dag_id="dag-complet-api-financiere-4",
    schedule="@once",
    start_date=datetime(2025, 6, 1),
    catchup=False,
)
def taskflow():
    @task(
        task_id="extract",
        retries=2,
    )
    def extract_bitcoin_price(task_instance) -> Dict[str, float]:
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
            dates.append(formatted_date)
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
            "price": prices[-1],
            "rsi": rsi,
        }

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: {data['price']} with rsi {data['rsi']}")

    store_data(process_data(extract_bitcoin_price()))


taskflow()
