from datetime import datetime, timedelta
from http import HTTPStatus
import logging
from pathlib import Path
from tempfile import mkstemp

from airflow.sdk import dag, task, BaseHook, Variable
from airflow.sdk.exceptions import AirflowFailException
import httpx
import boto3
from botocore.exceptions import ClientError

DATASET_PATH = "dataset.csv"
SVC_CONN_NAME = "openweathermap"
STORAGE_CONN_NAME = "rustfs"
DATASET_BUCKET = 'mirek'

logger = logging.getLogger(__name__)


@task.bash
def is_service_alive_in_bash():
    return "ping -c 1 -w 2 openweathermap.org"


@task(task_display_name="RustFS Healthcheck")
def is_rustfs_alive():
    conn = BaseHook.get_connection(STORAGE_CONN_NAME)

    response = httpx.head(f"{conn.schema}://{conn.host}:{conn.port}/health")

    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException(
            f"RustFS is unhealthy. Status code: {response.status_code}"
        )


@task(
    task_display_name="Service Healthcheck",
    retries=3,
    retry_delay=timedelta(seconds=10),
)
def is_service_alive():
    conn = BaseHook.get_connection(SVC_CONN_NAME)

    response = httpx.head(
        f"{conn.schema}://{conn.host}:{conn.port}", follow_redirects=True
    )

    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException(
            f"Service unavailable. HTTP status code: {response.status_code}"
        )


@task(task_display_name="Scrape Data")
def scrape_data(query: str) -> dict:
    """
    Scrape weather data from the openweathermap.org.

    @return: A dictionary containing the scraped weather data as JSON (dictionary).
    """
    logger.info("Scraping Data")

    conn = BaseHook.get_connection(SVC_CONN_NAME)

    # prepare query params and url
    url = f"{conn.schema}://{conn.host}:{conn.port}/data/2.5/weather"
    params = {
        "q": query,
        "units": conn.extra_dejson.get("units"),
        "appid": conn.password,
    }

    response = httpx.get(url, params=params)

    if response.status_code == HTTPStatus.UNAUTHORIZED:
        logger.error(
            'Error "401 Unauthorized". Please check your API key and try again.'
        )
        raise AirflowFailException(
            'Error "401 Unauthorized". Please check your API key and try again.'
        )

    elif response.status_code == HTTPStatus.NOT_FOUND:
        logger.error(
            f'Error "404 Not Found". The city "{query}" was not found. Please check the city name and try again.'
        )
        raise AirflowFailException(
            f'Error "404 Not Found". The city "{query}" was not found. Please check the city name and try again.'
        )

    if response.status_code != HTTPStatus.OK:
        logger.error(
            f'Error "{response.status_code}" while fetching data from openweathermap.org'
        )
        logger.error(response.json()["message"])
        raise AirflowFailException(
            f'Error "{response.status_code}" while fetching data from openweathermap.org'
        )

    return response.json()


@task(task_display_name="Process Data")
def process_data(data: dict) -> str:
    """
    Process and transform the scraped weather data.

    @param data: A dictionary containing the scraped weather data as JSON (dictionary).
    @return: A string containing the processed weather data in CSV format.
    """
    logger.info("Processing Data")

    return "{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
        data["dt"],
        data["name"],
        data["sys"]["country"],
        data["sys"]["sunrise"],
        data["sys"]["sunset"],
        data["main"]["temp"],
        data["main"]["temp_min"],
        data["main"]["temp_max"],
        data["main"]["humidity"],
        data["weather"][0]["description"],
        data["weather"][0]["main"],
        data["wind"]["speed"],
        data["wind"]["deg"],
    )


@task(task_display_name="Publish Data")
def publish_data(entry: str):
    """
    Publish the processed weather data as CSV file.

    @param entry: A string containing the processed weather data in CSV format.
    """
    logger.info("Publishing Data")

    conn = BaseHook.get_connection(STORAGE_CONN_NAME)

    storage = boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
    bucket = storage.Bucket(DATASET_BUCKET)

    path = Path(mkstemp(prefix='weather-')[1])

    # download dataset to temporary file
    try:
        bucket.download_file('dataset.csv', path)
    except ClientError:
        logger.warning("Dataset file doesn't exist yet. Possible first time upload")

        # add header
        with open(path, 'w') as file:
            print(
                 "dt,name,country,sunrise,sunset,temp,temp_min,temp_max,humidity,description,main,wind_speed,wind_deg",
                 file=file,
            )

    # append new measurement as line
    with open(path, 'a') as file:
        print(entry, file=file)

    # upload dataset
    bucket.upload_file(path, 'dataset.csv')

    # cleanup
    path.unlink(missing_ok=True)


@dag(
    "weather_scraper",
    dag_display_name="Weather Scraper",
    description="Scrapes weather data from openweathermap.org.",
    schedule="*/20 * * * *",
    start_date=datetime(2026, 6, 1),
    tags=["mirek", "training", "dt"],
    catchup=False,
)
def main(query: str = Variable.get("WEATHER_CITY")):
    data = [
        is_service_alive_in_bash(),
        is_rustfs_alive(),
        is_service_alive(),
    ] >> scrape_data(query)
    csv_entry = process_data(data)
    publish_data(csv_entry)


if __name__ == "__main__":
    main().test()
else:
    main()
