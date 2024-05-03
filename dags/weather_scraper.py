import json
from pathlib import Path
import sys
from http import HTTPStatus
import logging
import tempfile

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
from pendulum import datetime
import jsonschema
from sh import ping
from botocore.exceptions import ClientError

from helpers import get_minio
from tasks import healthcheck_minio


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
    minio = get_minio()
    bucket = minio.Bucket('datasets')
    
    # create temporary file
    path = Path(tempfile.mkstemp()[1])

    # download dataset    
    try:
        bucket.download_file('dataset.csv', path)
    except ClientError:
        logger.warning('Dataset not found. Possibly first run.')

    # append measurement
    with open(path, "a") as file:
        print(line, file=file)
        
    # upload dataset
    bucket.upload_file(path, 'dataset.csv')
    
    # remove temporary file
    path.unlink(True)


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
