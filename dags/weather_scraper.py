from datetime import datetime, timedelta
from http import HTTPStatus
import logging
from pathlib import Path
from tempfile import mkstemp

from airflow.sdk import dag, task, BaseHook, Variable, Param, get_current_context
from airflow.sdk.exceptions import AirflowFailException
import httpx
from botocore.exceptions import ClientError
import pendulum

from tasks import is_rustfs_alive
from constants import SVC_CONN_NAME, DATASET_BUCKET
from helpers import get_s3


logger = logging.getLogger(__name__)


@task.bash
def is_service_alive_in_bash():
    return "ping -c 1 -w 2 openweathermap.org"


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
        pendulum.from_timestamp(data["dt"]).to_iso8601_string(),
        data["name"],
        data["sys"]["country"],
        pendulum.from_timestamp(data["sys"]["sunrise"]).to_iso8601_string(),
        pendulum.from_timestamp(data["sys"]["sunset"]).to_iso8601_string(),
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

    # get ready
    storage = get_s3()
    bucket = storage.Bucket(DATASET_BUCKET)
    path = Path(mkstemp(prefix="weather-")[1])
    parts = entry.split(',')
    dataset_file = f'{parts[1]}-{parts[2]}.csv'.lower()

    # download dataset to temporary file
    try:
        bucket.download_file(dataset_file, path)
    except ClientError:
        logger.warning("Dataset file doesn't exist yet. Possible first time upload")

        # add header
        with open(path, "w") as file:
            print(
                "dt,name,country,sunrise,sunset,temp,temp_min,temp_max,humidity,description,main,wind_speed,wind_deg",
                file=file,
            )

    # append new measurement as line
    with open(path, "a") as file:
        print(entry, file=file)

    # upload dataset
    bucket.upload_file(path, dataset_file)

    # cleanup
    path.unlink(missing_ok=True)

@task
def get_locations():
    context = get_current_context()
    
    query = context['params']['query']
    if query == []:
        return Variable.get('WEATHER_CITY').splitlines()
    else:
        return query


@dag(
    "weather_scraper",
    dag_display_name="Weather Scraper",
    description="Scrapes weather data from openweathermap.org.",
    schedule="*/20 * * * *",
    start_date=datetime(2026, 6, 1),
    tags=["mirek", "training", "dt"],
    catchup=True,
    params={
        "query": Param(
            type='array', 
            default=[], 
            title='Locations', 
            description='List of locations. One location per line.'
        )
    }
)
def main(): 
    locations = get_locations()

    data = [
        is_service_alive_in_bash(),
        is_rustfs_alive(),
        is_service_alive(),
    ] >> scrape_data.expand(query=locations)
    csv_entry = process_data.expand(data=data)
    publish_data.expand(entry=csv_entry)


if __name__ == "__main__":
    main().test()
else:
    main()
