import json
from pathlib import Path
import sys
from http import HTTPStatus
import logging

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
from pendulum import datetime
import jsonschema
from sh import ping


logger = logging.getLogger(__name__)


@task
def scrape_data(query):
    """
    Scrapes the data from openweathermap.org
    """
    logger.info(">> Scraping Data")

    conn = BaseHook.get_connection("openweathermap")

    url = f"{conn.schema}://{conn.host}/data/2.5/weather"
    params = {"q": query, "appid": conn.password, "units": "metric"}
    response = httpx.get(url, params=params)

    if response.status_code == HTTPStatus.NOT_FOUND:
        raise AirflowFailException("City not found.")
        # sys.exit("Error: City not found.")

    if response.status_code == HTTPStatus.UNAUTHORIZED:
        sys.exit("Error: Invalid API Key.")

    data = response.json()
    return data


@task
def process_data(data):
    """
    Process the passed data
    """
    logger.info(">> Processing Data")

    return "{},{},{},{},{},{},{}".format(
        data["dt"],
        data["name"],
        data["main"]["temp"],
        data["main"]["pressure"],
        data["main"]["humidity"],
        data["wind"]["speed"],
        data["wind"]["deg"],
    )


@task
def publish_data(line):
    """
    Publish/persist the data to CSV file
    """
    logger.info(">> Publishing Data")

    path = Path(__file__).parent / "dataset.csv"

    with open(path, "a") as file:
        print(line, file=file)


@task
def validate_data(data: dict):
    path = Path(__file__).parent / "weather.schema.json"

    with open(path, "r") as file:
        schema = json.load(file)
        jsonschema.validate(data, schema)
        return data


@task
def healthcheck_weather():
    conn = BaseHook.get_connection("openweathermap")
    ping(conn.host, "-c", "1", _timeout=3)
    
    
@task
def healthcheck_minio():
    url = 'http://localhost:9000/minio/health/live'
    response = httpx.get(url)
    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException('MinIO service is unhealthy.')


@dag(
    "weather_scraper",
    description="Scrapes weather from openweathermap.org",
    schedule="*/20 * * * *",
    start_date=datetime(2024, 4, 1),
    tags=["weather", "devops", "t-sys", "tuke"],
    catchup=False
)
def main(query: str = "kosice"):
    
    measurement = [
        healthcheck_minio(),
        healthcheck_weather()
    ] >> scrape_data(query)
    valid_data = validate_data(measurement)
    line = process_data(valid_data)
    publish_data(line)


main()
