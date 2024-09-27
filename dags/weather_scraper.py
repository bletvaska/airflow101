from http import HTTPStatus
import json
import logging
from pathlib import Path
import tempfile

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
from pendulum import datetime
from sh import ping
from jsonschema import validate
import boto3
import botocore

from properties import DATASETS_BUCKET, S3_CONN_NAME, OWM_CONN_NAME


logger = logging.getLogger(__name__)


@task
def scrape_data(query: str) -> dict:
    """
    Scrapes data from external source.
    """
    logger.info(">> Scraping Data")

    conn = BaseHook.get_connection(OWM_CONN_NAME)
    url = f"{conn.schema}://{conn.host}:{conn.port}/data/2.5/weather"
    params = {"appid": conn.password, "q": query, "units": conn.extra_dejson["units"]}

    response = httpx.get(url, params=params)

    if response.status_code == HTTPStatus.NOT_FOUND:
        logger.error(f'City "{query}" not found.')
        raise AirflowFailException(f'City "{query}" not found.')

    elif response.status_code == HTTPStatus.UNAUTHORIZED:
        logger.error("Invalid API key.")
        raise AirflowFailException("Invalid API key.")

    data = response.json()
    return data


@task
def process_data(data: dict) -> str:
    """
    Processes and filters measurement data.
    """
    logger.info(">> Processing Data")
    result = "{},{},{},{},{},{},{},{}".format(
        data["dt"],
        data["sys"]["country"],
        data["name"],
        data["main"]["temp"],
        data["main"]["humidity"],
        data["main"]["pressure"],
        data["wind"]["speed"],
        data["wind"]["deg"],
    )
    return result


@task
def publish_data(line: str):
    """
    Saves measurement to CSV file.
    """
    logger.info(">> Publishing Data")

    conn = BaseHook.get_connection(S3_CONN_NAME)
    minio = boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
    bucket = minio.Bucket(DATASETS_BUCKET)
    _, filename = tempfile.mkstemp()
    tmpfile = Path(filename)

    # stiahni dataset.csv z S3/Minio
    try:
        bucket.download_file("dataset.csv", tmpfile)
    except botocore.exceptions.ClientError:
        logger.warning("Dataset doesn't exist in bucket. Possible first time upload.")

    # pripoj k nemu posledne meranie
    with open(tmpfile, mode="a") as dataset:
        print(line, file=dataset)

    # uploadni dataset.csv naspat do S3/Minio
    bucket.upload_file(tmpfile, "dataset.csv")

    # zmaz docasne stiahnuty subor
    tmpfile.unlink(True)


@task(retries=3)
# @task.bash
def is_service_alive():
    logger.info(">> Healthcheck")
    conn = BaseHook.get_connection(OWM_CONN_NAME)
    ping("-c", 1, conn.host, _timeout=2)
    # return 'ping -c 1 -w 2 api.openweathermap.org'


@task(retries=3)
def is_minio_alive():
    logger.info(">> MinIO Healthcheck")
    conn = BaseHook.get_connection(S3_CONN_NAME)
    url = f"{conn.schema}://{conn.host}:{conn.port}/minio/health/live"
    response = httpx.head(url)

    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException("Minio server not available")


@task
def validate_data(data: dict) -> dict:
    path = Path(__file__).parent / "weather.schema.json"

    with open(path) as schema_file:
        schema = json.load(schema_file)
        validate(instance=data, schema=schema)
        return data


@dag(
    "weather_scraper",
    dag_display_name="Weather Scraper",
    description="Scrapes weather from openweathermap.org",
    schedule="*/20 * * * *",
    start_date=datetime(2024, 9, 24),
    tags=["weather", "devops", "dtit"],
    catchup=False,
)
def main(query: str = "kosice,sk"):
    # scrape_data | process_data | publish_data
    measurement = [is_minio_alive(), is_service_alive()] >> scrape_data(query)
    validated_data = validate_data(measurement)
    line = process_data(validated_data)
    publish_data(line)


if __name__ == "__main__":
    main().test()
else:
    main()
