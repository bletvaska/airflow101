# standard packages
from http import HTTPStatus
import json
from pathlib import Path
import logging
from tempfile import mkstemp

# third-party packages
from airflow.sdk import dag, task, BaseHook, Variable
from airflow.exceptions import AirflowFailException
import boto3
import botocore
from pendulum import datetime
import pendulum
import httpx
import jsonschema
from apprise import Apprise

# own packages

logger = logging.getLogger(__name__)


@task(task_display_name="Check MinIO Availability")
def is_minio_alive():
    """
    Checks if the MinIO service is alive using the liveness endpoint.
    """
    logger.info("Checking MinIO availability")

    conn = BaseHook.get_connection("minio")
    url = f"{conn.schema}://{conn.host}:{conn.port}/minio/health/live"
    response = httpx.get(url, timeout=5)

    if response.status_code != HTTPStatus.OK:
        logger.error("MinIO is unhealthy. Nothing to do. Quit.")
        raise AirflowFailException("MinIO is unhealthy. Quit.")


@task.bash
def ping_service():
    logger.info("pinging remote service")

    conn = BaseHook.get_connection("openweathermap")
    cmd = f"ping -c 1 -w 2 {conn.host}"
    return cmd


@task(task_display_name="Service Check")
def is_service_alive():
    logger.info("Checking Openweathermap.org")

    conn = BaseHook.get_connection("openweathermap")

    url = f"{conn.schema}://{conn.host}:{conn.port}"
    response = httpx.get(url)

    logger.info(response.status_code)
    if response.status_code != HTTPStatus.MOVED_PERMANENTLY:
        raise AirflowFailException(
            f"Unexpected HTTP status code ({response.status_code})"
        )


@task(task_display_name="Scrape Data")
def scrape_data(query: str, units: str) -> dict:
    """
    Scrapes data from a specified source.
    """
    logger.info("Scraping Data")

    conn = BaseHook.get_connection("openweathermap")

    url = f"{conn.schema}://{conn.host}:{conn.port}"
    path = "data/2.5/weather"
    params = f"appid={conn.password}&q={query}&units={units}"

    response = httpx.get(f"{url}/{path}?{params}")
    if response.status_code != HTTPStatus.OK:
        logger.error(f"HTTP status code is {response.status_code}")
        # quit
        raise AirflowFailException(
            f"Unexpected HTTP status code ({response.status_code})"
        )

    data = response.json()

    return data


@task(task_display_name="Process Data")
def process_data(data: dict) -> str:
    """
    Processes the scraped data.
    """
    logger.info("Processing Data")

    return "{};{};{};{};{};{};{};{};{};{};{}".format(
        data["main"]["temp"],
        data["main"]["humidity"],
        data["main"]["pressure"],
        data["name"],
        data["sys"]["country"],
        data["visibility"],
        data["wind"]["speed"],
        data["wind"]["deg"],
        data["sys"]["sunset"],
        data["sys"]["sunrise"],
        data["weather"][0]["description"],
    )


@task(task_display_name="Publish Data")
def publish_data(entry: str):
    """
    Publishes the processed data to a specified destination.
    """
    logger.info("Publishing Data")

    # create minio object
    conn = BaseHook.get_connection("minio")
    minio = boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
    bucket = minio.Bucket("datasets")
    path = Path(mkstemp()[1])

    # download dataset
    try:
        bucket.download_file("kosice.csv", path)
    except botocore.exceptions.ClientError:
        logger.warning("Dataset doesn't exist in bucket. Possible first time upload.")

    # append new entry to dataset
    with open(path, "a") as dataset:
        print(entry, file=dataset)

    # upload dataset
    bucket.upload_file(path, "kosice.csv")

    # clean
    path.unlink(True)


@task(task_display_name="Validate JSON Data")
def validate_data(data: dict):
    logger.info("Validating JSON Data")

    path = Path(__file__).parent.parent / "weather.schema.json"
    with open(path, "r") as file:
        schema = json.load(file)
        jsonschema.validate(instance=data, schema=schema)

    return data

@task(task_display_name="Notification")
def notify(entry: str):
    logger.info('Notification of client.')

    # get ready
    token = Variable.get('PUSHBULLET_TOKEN')
    parts = entry.split(';')
    sunset = pendulum.from_timestamp(1761147163).in_timezone('Europe/Bratislava').to_time_string()

    text = f'Aktuálna situácia na mieste {parts[3]}({parts[4]}) je: teplota {parts[0]}°C, vlhkosť {parts[1]}%, tlak {parts[2]}hPa. Celková situácia je {parts[10]}. Slnko dnes zapadá o {sunset}.'

    # send notification
    apprise = Apprise()
    apprise.add(f'pbul://{token}')
    apprise.notify(
        title='Aktuálne počasie',
        body=text
    )




@dag(
    "weather_scraper",
    dag_display_name="Weather Scraper",
    description="A DAG to scrape weather data from openweathermap.org.",
    schedule="*/20 * * * *",
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["weather", "devops", "python", "dt"],
)
def main(query: str = "kosice,sk", units: str = "metric"):
    # ping_service()

    data = [is_minio_alive(), is_service_alive()] >> scrape_data(query, units)
    validated_data = validate_data(data)
    csv_entry = process_data(validated_data)
    publish_data(csv_entry)
    notify(csv_entry)


if __name__ == "__main__":
    main().test()
else:
    main()
