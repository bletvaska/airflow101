from http import HTTPStatus
import json
from pathlib import Path
import logging
from tempfile import mkstemp

from airflow.sdk import dag, task, BaseHook
from airflow.exceptions import AirflowFailException
import boto3
import botocore
from pendulum import datetime
import httpx
import jsonschema

logger = logging.getLogger(__name__)


@task(task_display_name="Check MinIO Availability")
def is_minio_alive():
    """
    Checks if the MinIO service is alive using the liveness endpoint.
    """
    logger.info("Checking MinIO availability")
    try:
        conn = BaseHook.get_connection("minio")
        url = f"{conn.schema}://{conn.host}:{conn.port}/minio/health/live"
        response = httpx.get(url, timeout=5)
        if response.status_code != 200:
            logger.error(
                f"MinIO returned status {response.status_code}: {response.text}"
            )
            quit()
    except Exception as e:
        logger.error("Failed to reach MinIO service")
        logger.exception(e)
        quit()


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
        # quit()
        raise AirflowFailException(f'Unexpected HTTP status code ({response.status_code})')


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
        logger.error(f'HTTP status code is {response.status_code}')
        # quit
        raise AirflowFailException(f'Unexpected HTTP status code ({response.status_code})')

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
        aws_secret_access_key=conn.password
    )
    bucket = minio.Bucket('datasets')
    path = Path(mkstemp()[1])

    # download dataset
    try:
        bucket.download_file('kosice.csv', path)
    except botocore.exceptions.ClientError:
        logger.warning("Dataset doesn't exist in bucket. Possible first time upload.")

    # append new entry to dataset
    with open(path, "a") as dataset:
        print(entry, file=dataset)

    # upload dataset
    bucket.upload_file(path, 'kosice.csv')

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


if __name__ == "__main__":
    main().test()
else:
    main()
