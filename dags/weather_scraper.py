import logging
from pathlib import Path
import tempfile

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
from pendulum import datetime
import botocore

from helpers import get_minio
from tasks import is_minio_alive

logger = logging.getLogger(__file__)
DATASET = "weather.csv"


@task
def scrape_data(query: str) -> dict:
    logger.info(f"Scraping data for {query}")

    connection = BaseHook.get_connection("openweathermap")

    params = {"q": query, "units": "metric", "appid": connection.get_password()}

    response = httpx.get(connection.host, params=params)
    if response.status_code != 200:
        logger.error("Request error")
        raise AirflowFailException("Request Error")

    return response.json()


@task
def process_data(data: dict) -> str:
    logger.info("Processing data")

    line = "{},{},{},{},{},{},{},{},{},{}".format(
        data["dt"],
        data["name"],
        data["sys"]["country"],
        data["main"]["temp"],
        data["main"]["humidity"],
        data["main"]["pressure"],
        data["sys"]["sunrise"],
        data["sys"]["sunset"],
        data["wind"]["deg"],
        data["wind"]["speed"],
    )

    return line


@task
def publish_data(line: str):
    logger.info("Publishing data")

    minio = get_minio()

    tmpfile = tempfile.mkstemp()[1]
    path = Path(tmpfile)
    logger.info(f"Downloading dataset to file {path}")

    bucket = minio.Bucket("datasets")
    try:
        bucket.download_file(DATASET, path)
    except botocore.exceptions.ClientError:
        logger.warning("Dataset doesn't exist in bucket. Possible first time upload.")

    with open(path, "a") as dataset:
        print(line, file=dataset)

    bucket.upload_file(path, DATASET)
    path.unlink()


@task
def is_service_alive():
    try:
        connection = BaseHook.get_connection("openweathermap")
        params = {"appid": connection.get_password()}
        response = httpx.get(connection.host, params=params)
        if response.status_code == 401:
            raise AirflowFailException("Invalid API Key")
    except httpx.ConnectError:
        logger.error(f"Invalid hostname {connection.host}")
        raise AirflowFailException("Invalid host name.")


@dag(
    "weather_scraper",
    description="Scrapes weather from openweathermap.org",
    schedule="*/20 * * * *",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    tags=["weather", "devops", "telekom"],
    catchup=False,
)
def main(query: str = "kosice,sk"):
    # is_service_alive | scrape_data | process_data | publish_data

    data = [is_minio_alive(), is_service_alive()] >> scrape_data(query)
    processed_data = process_data(data)
    publish_data(processed_data)


main()
