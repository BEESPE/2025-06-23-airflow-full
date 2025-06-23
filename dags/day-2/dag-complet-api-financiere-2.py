from datetime import datetime
import logging
import requests
from typing import Dict

from airflow.decorators import dag, task

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}&localization=fr"

@dag(
    dag_id="dag-complet-api-financiere-2",
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
        formatted_date = task_instance.execution_date.strftime("%d-%m-%Y")
        return requests.get(API.format(formatted_date)).json()["market_data"]

    @task(multiple_outputs=True)
    def process_data(response: Dict[str, float]) -> Dict[str, float]:
        logging.info(response)
        return {
            "price": response["current_price"]["usd"],
            "volume": response["total_volume"]["usd"],
        }

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: {data['price']} with volume {data['volume']}")

    store_data(process_data(extract_bitcoin_price()))


taskflow()
