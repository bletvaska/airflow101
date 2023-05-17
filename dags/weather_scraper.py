import json
from pathlib import Path
import tempfile
import logging

import httpx
import jsonschema
from pendulum import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
import boto3
import botocore


# create logger
logger = logging.getLogger(__name__)  # weather_scraper

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
        logger.info("is weather alive")

        response = httpx.head(url, params=params)

        if response.status_code != 200:
            logger.critical("Invalid API key.")
            raise AirflowFailException("Invalid API key.")

    @task
    def is_minio_alive():
        logger.info("is minio alive")

        base_url = BaseHook.get_connection("minio").host
        response = httpx.head(f"{base_url}/minio/health/live")

        if response.status_code != 200:
            logger.critical("Minio is not Alive")
            raise AirflowFailException("MinIo is not Alive.")

    @task
    def scrape_data() -> dict:
        logger.info("downloading data")

        # scrape data
        response = httpx.get(url, params=params)

        return response.json()

    @task
    def process_data(data: dict) -> str:
        logger.info("processing data")

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
        logger.info("uploading data")

        # get ready
        minio_conn = BaseHook.get_connection('minio')
        minio = boto3.resource(
            "s3",
            endpoint_url=minio_conn.host,
            aws_access_key_id=minio_conn.login,
            aws_secret_access_key=minio_conn.password,
        )
        
        # download file from s3 to (temporary file)
        bucket = minio.Bucket('datasets')
        
        # doesn't exist?
        path = Path(tempfile.mkstemp()[1])
        logger.debug(f'Downloading to file {path}.')
        
        try:
            bucket.download_file('dataset.csv', path)
        except botocore.exceptions.ClientError as ex:
            logger.warning("Dataset doesn't exist in bucket. Possible first time upload.")
            
        # update by appending new line
        with open(path, mode="a") as file:
            file.write(f"{entry}\n")

        # upload updated file to s3
        bucket.upload_file(path, 'dataset.csv')

        # (cleanup)
        path.unlink()

    @task
    def validate_json_data(data: dict):
        logger.info('json data validation')
        
        # airflow directory
        path = Path(__file__).parent.parent

        # read schema
        with open(path / "weather.schema.json", mode="r") as file:
            schema = json.load(file)

        # validate
        jsonschema.validate(instance=data, schema=schema)

        return data

    # DAG Definition
    # is_weather_alive | scrape_data | process_data | upload_data
    raw_data = [is_weather_alive(), is_minio_alive()] >> scrape_data()
    valid_data = validate_json_data(raw_data)
    entry = process_data(valid_data)
    update_dataset(entry)
