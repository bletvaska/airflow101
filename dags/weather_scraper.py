import json
import logging
from http import HTTPStatus

import httpx
from airflow.sdk import BaseHook, Variable, dag, task
from airflow.sdk.exceptions import AirflowFailException
from jsonschema import validate
from pendulum import datetime, from_timestamp
from sh import ping

from constants import DATA_PATH


logger = logging.getLogger(__name__)


@task.bash
def is_service_alive():
    logger.info("Checking status of the service")

    conn = BaseHook.get_connection("openweathermap")
    return f'ping -c 1 -w 2 {conn.host}'


@task
def is_service_alive_2():
    logger.info("Checking status of the service with sh module")

    conn = BaseHook.get_connection("openweathermap")
    ping('-c', '1', conn.host, _timeout=2)


@task
def scraping_data(query: str) -> dict:
    """
    Scrapes the data from openweathermap.org
    """
    logger.info("Scraping Data")

    conn = BaseHook.get_connection("openweathermap")
    url = f"{conn.schema}://{conn.host}:{conn.port}/data/2.5/weather"
    params = {
        "q": query,
        "appid": conn.password,
        "units": conn.extra_dejson.get("units"),
    }

    response = httpx.get(url, params=params)

    if response.status_code != HTTPStatus.OK:
        logger.warning('Something wrong happend.')
        raise AirflowFailException('ta daco nedobre')

    return response.json()


@task
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


@task
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


@task
def publishing_data(line: str):
    """
    Persist the data
    """
    logger.info("Publishing Data")

    with open(DATA_PATH / "dataset.csv", mode="a") as dataset:
        print(line, file=dataset)


@dag(
    "weather_scraper",
    dag_display_name="Weather Scraper",
    description="Scrapes weather from openweathermap.org",
    tags=["weather", "devops", "dt", "training"],
    schedule="*/20 * * * *",
    start_date=datetime(2026, 9, 22),
    end_date=datetime(2026, 9, 30),
    catchup=False,
)
def main(query: str = Variable.get("weather_city")):
    data = is_service_alive_2() >> scraping_data(query)
    valid_data = validate_data(data)
    csv_entry = processing_data(valid_data)
    publishing_data(csv_entry)


if __name__ == "__main__":
    main().test()
else:
    main()
