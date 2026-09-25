import json
import logging
from http import HTTPStatus
from datetime import timedelta
import tempfile
from pathlib import Path

from botocore.exceptions import ClientError
import httpx
from airflow.sdk import BaseHook, Variable, dag, task, task_group, Param
from airflow.sdk.exceptions import AirflowFailException
from jsonschema import validate
from pendulum import datetime, from_timestamp
from sh import ping

from constants import DATA_PATH, WEATHER_CONN, BUCKET_NAME, DATASET_FILE
from assets import WEATHER_DATA
from tasks import is_rustfs_alive
from helpers import get_storage


logger = logging.getLogger(__name__)


@task.bash
def is_service_alive():
    logger.info("Checking status of the service")

    conn = BaseHook.get_connection(WEATHER_CONN)
    return f"ping -c 1 -w 2 {conn.host}"


@task(retries=3, retry_delay=timedelta(seconds=10))
def is_service_alive_2():
    logger.info("Checking status of the service with sh module")

    conn = BaseHook.get_connection(WEATHER_CONN)
    ping("-c", "1", conn.host, _timeout=2)


@task(task_display_name="Scrape Data")
def scraping_data(query: str) -> dict:
    """
    Scrapes the data from openweathermap.org
    """
    logger.info("Scraping Data")

    conn = BaseHook.get_connection(WEATHER_CONN)
    url = f"{conn.schema}://{conn.host}:{conn.port}/data/2.5/weather"
    params = {
        "q": query,
        "appid": conn.password,
        "units": conn.extra_dejson.get("units"),
    }

    response = httpx.get(url, params=params)

    if response.status_code != HTTPStatus.OK:
        logger.warning("Something wrong happend.")
        raise AirflowFailException("ta daco nedobre")

    return response.json()


@task(task_display_name="Validate Data")
def validate_data(json_data: dict) -> dict:
    """
    validate the downloaded data
    """
    logger.info("Validating data")

    with open(DATA_PATH / "openweathermap.schema.json", "r") as file:
        schema = json.load(file)
    validate(instance=json_data, schema=schema)

    # return None
    return json_data


@task(task_display_name="Process Data")
def processing_data(json_data: dict) -> str:
    """
    Process the downloaded data
    """
    logger.info("Processing Data")

    # 'kedy;mesto;krajina;teplota;vlhkost;tlak;rychlost vetra;smer vetra'
    dt = json_data["dt"]
    return "{};{};{};{};{};{};{};{}".format(
        from_timestamp(dt).to_iso8601_string(),
        json_data["name"],
        json_data["sys"]["country"],
        json_data["main"]["temp"],
        json_data["main"]["humidity"],
        json_data["main"]["pressure"],
        json_data["wind"]["speed"],
        json_data["wind"]["deg"],
    )


@task(
    task_display_name="Publish Data",
    outlets=[WEATHER_DATA],
)
def publishing_data(line: str):
    """
    Persist the data
    """
    logger.info("Publishing Data")

    storage = get_storage()

    bucket = storage.Bucket(BUCKET_NAME)

    try:
        path = Path(tempfile.mkstemp()[1])

        try:
            bucket.download_file(DATASET_FILE, path)
        except ClientError as ex:
            logger.warning("Dataset file not found. Probably first dataset upload.")
            with open(path, "w") as file:
                print(
                    "dt;name;country;temp;humidity;pressure;wind speed;wind angle",
                    file=file,
                )

        with open(path, "a") as file:
            print(line, file=file)

        bucket.upload_file(path, DATASET_FILE)
    finally:
        path.unlink(True)


@task_group(
    group_id="tg_healthcheck",
    group_display_name="Healthcheck",
    tooltip="Healthcheck of external services",
)
def tg_healthcheck():
    return [is_rustfs_alive(), is_service_alive(), is_service_alive_2()]


@task_group(
    "tg_weather_ingestion",
    group_display_name="Weather Ingestion",
    tooltip="Retrieve, process and upload weather info.",
)
def tg_weather_ingestion(query: str):
    data = scraping_data(query)
    valid_data = validate_data(data)
    csv_entry = processing_data(valid_data)
    publishing_data(csv_entry)


@dag(
    "weather_scraper",
    dag_display_name="Weather Scraper",
    description="Scrapes weather from openweathermap.org",
    tags=["weather", "devops", "dt", "training"],
    schedule="*/20 * * * *",
    start_date=datetime(2026, 9, 22),
    end_date=datetime(2026, 9, 30),
    catchup=False,
    params={
        'query': Param(
            default=Variable.get('weather_city').split(),
            type='array',
            title='Locations',
            description='List of locations. One location per line.'
        )
    }
)
def main():
    tg_healthcheck() # >> tg_weather_ingestion(query)



if __name__ == "__main__":
    main().test()
else:
    main()
