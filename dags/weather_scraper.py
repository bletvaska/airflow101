import json
from pathlib import Path

import httpx
import jsonschema
from pendulum import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook

base_url = BaseHook.get_connection("openweathermap").host

url = f"{base_url}/data/2.5/weather"
params = {
    "q": "kosice",
    "appid": BaseHook.get_connection("openweathermap").password,
    "units": "metric",
}

with DAG(
    "weather_scraper",
    description="Scrapes weather from openweathermap.org service.",
    schedule="*/20 * * * *",
    start_date=datetime(2023, 4, 29),
    catchup=False,
    tags=["training", "t-systems", "weather"],
):

    @task
    def is_weather_alive():
        print(">> is weather alive")

        response = httpx.head(url, params=params)

        if response.status_code != 200:
            print("Invalid API key.")
            raise AirflowFailException("Invalid API key.")

    @task
    def is_minio_alive():
        print(">> is minio alive")

        base_url = BaseHook.get_connection("minio").host
        response = httpx.head(f"{base_url}/minio/health/live")

        if response.status_code != 200:
            print("Minio is not Alive")
            raise AirflowFailException("MinIo is not Alive.")

    @task
    def scrape_data() -> dict:
        print(">> downloading data")

        # scrape data
        response = httpx.get(url, params=params)

        return response.json()

    @task
    def process_data(data: dict) -> str:
        print(">> processing data")

        # dt; main.temp; main.humidity; main.pressure; weather.main; visibility; wind.speed; wind.deg;
        return "{};{};{};{};{};{};{};{}".format(
            data["dt"],
            data["main"]["temp"],
            data["main"]["humidity"],
            data["main"]["pressure"],
            data["weather"][0]["main"],
            data["visibility"],
            data["wind"]["speed"],
            data["wind"]["deg"],
        )

    @task
    def update_dataset(entry: str):
        print(">> uploading data")
        
        # download file from s3 to (temporary file)

        # update by appending new line
        with open("dataset.csv", mode="a") as file:
            file.write(f"{entry}\n")
            
        # upload updated file to s3
        
        # (cleanup)
            
    @task
    def validate_json_data(data: dict):
        # airflow directory
        path = Path(__file__).parent.parent
        
        # read schema
        with open(path / 'weather.schema.json', mode='r') as file:
            schema = json.load(file)
            
        # validate
        jsonschema.validate(instance=data, schema=schema)
        
        return data

    # is_weather_alive | scrape_data | process_data | upload_data
    raw_data = [is_weather_alive(), is_minio_alive()] >> scrape_data()
    valid_data = validate_json_data(raw_data)
    entry = process_data(valid_data)
    update_dataset(entry)
