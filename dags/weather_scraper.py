import logging
from datetime import timedelta
from http import HTTPStatus
from pathlib import Path

import boto3
import httpx
from airflow.sdk import (
    BaseHook,
    Param,
    Variable,
    dag,
    get_current_context,
    task,
    task_group,
)
from airflow.sdk.exceptions import AirflowFailException
from pendulum import datetime, from_timestamp

logger = logging.getLogger(__name__)

RUSTFS_CONN_ID = "rustfs"
WEATHER_API_CONN_ID = "openweathermap"


@task(task_display_name="Scrape Data")
def scrape_data(query: str) -> dict:
    """
    Scrapes data from openweathermap.org
    """
    conn = BaseHook.get_connection(WEATHER_API_CONN_ID)

    url = f"{conn.schema}://{conn.host}/data/2.5/weather"
    params = {
        "q": query,
        "appid": conn.password,
        "units": conn.extra_dejson["units"],
    }

    response = httpx.get(url, params=params)
    if response.status_code != HTTPStatus.OK:
        data = response.json()
        logger.error(f"{response.status_code} - {data['message']}")
        raise AirflowFailException("Incorrect data")

    return response.json()


@task(task_display_name="Process Data")
def process_data(data: dict) -> dict:
    """
    Process the downloaded data
    """
    logger.info(">> Processing Data")

    result = {
        "dt": from_timestamp(data["dt"], "UTC").to_iso8601_string(),
        "city": data["name"],
        "country": data["sys"]["country"],
        "weather": data["weather"][0]["id"],
        "temp": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
    }

    return result


@task(task_display_name="Publish Data")
def publish_data(data: dict):
    """
    Data persistence
    """
    logger.info(">> Publishing Data")

    # line = data['dt'] + ',' + data['city'] + ',' + data['country'] + ',' + str(data['weather']) + ',' + str(data['temp']) + ',' + str(data['humidity']) + ',' + str(data['pressure'])
    # line = f"{data['dt']},{data['city']},{data['country']},{data['weather']},{data['temp']},{data['humidity']},{data['pressure']}"
    line = "{},{},{},{},{},{},{}".format(
        data["dt"],
        data["city"],
        data["country"],
        data["weather"],
        data["temp"],
        data["humidity"],
        data["pressure"],
    )

    # create datasets folder if not exist
    path = Path(__file__).parent.parent / "datasets"
    path.mkdir(parents=True, exist_ok=True)  # mkdir /path/to/folder --parents

    # append new line to dataset file
    with open(path / "dataset.csv", mode="a") as dataset:
        print(line, file=dataset)

    # upload dataset to s3/bucket
    conn = BaseHook.get_connection(RUSTFS_CONN_ID)
    storage = boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
    bucket = storage.Bucket("mirek")
    bucket.upload_file(path / "dataset.csv", "dataset.csv")


@task.bash(
    task_display_name="Openweathermap Healthcheck",
    execution_timeout=timedelta(seconds=5),
    retries=3,
    retry_delay=timedelta(seconds=10),
)
def is_service_alive():
    conn = BaseHook.get_connection(WEATHER_API_CONN_ID)
    return f"ping -c 1 -w 2 {conn.host}"


@task(
    task_display_name="Ping with sh",
    execution_timeout=timedelta(seconds=5),
    retries=3,
    retry_delay=timedelta(seconds=10),
)
def ping_with_sh():
    from sh import ping

    conn = BaseHook.get_connection(WEATHER_API_CONN_ID)
    ping("-c", "1", "-w", "2", conn.host)


@task(
    task_display_name="HTTP HEAD with httpx",
    execution_timeout=timedelta(seconds=5),
    retries=3,
    retry_delay=timedelta(seconds=10),
)
def http_head():
    conn = BaseHook.get_connection(WEATHER_API_CONN_ID)

    response = httpx.head(f"{conn.schema}://{conn.host}")
    print(f"{conn.schema}://{conn.host}")
    print(response.status_code)
    if response.status_code != HTTPStatus.MOVED_PERMANENTLY:
        raise AirflowFailException("Incorrect HTTP Status code")


@task
def get_location():
    context = get_current_context()

    # location = context['params']['query'] or Variable.get('weather_location')
    location = context["params"]["query"]
    if location is None:
        location = Variable.get("weather_location")

    return location


@task_group(group_display_name="Healthcheck")
def tg_healthcheck():
    return [is_service_alive(), ping_with_sh(), http_head()]


@dag(
    "weather_scraper",
    dag_display_name="Weather Scraper 20m",
    description="Scrapes weather from openweathermap.org",
    schedule="*/20 * * * *",
    start_date=datetime(2026, 10, 27),
    end_date=datetime(2026, 12, 24),
    tags=["weather", "training", "dt"],
    catchup=False,
    params={
        "query": Param(
            type=["string", "null"],
            title="Locations",
            description="The name of location to retrieve weather info about.",
            default=None,
        )
    },
)
def main():
    query = get_location()
    measurement = tg_healthcheck() >> scrape_data(query)
    entry = process_data(measurement)
    publish_data(entry)


#    CTRL+Space - intellisence

if __name__ == "__main__":
    main().test()
else:
    main()
