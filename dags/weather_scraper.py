from http import HTTPStatus
import logging

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
from pendulum import datetime
from sh import ping

logger = logging.getLogger(__name__)


@task
def scrape_data(query: str) -> dict:
    """
    Scrapes data from external source.
    """
    logger.info(">> Scraping Data")

    conn = BaseHook.get_connection("openweathermap")
    url = f"{conn.schema}://{conn.host}:{conn.port}/data/2.5/weather"
    params = {"appid": conn.password, "q": query, "units": "metric"}

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
    with open("dataset.csv", mode="a") as dataset:
        print(line, file=dataset)


@task(retries=3)
# @task.bash
def is_service_alive():
    logger.info(">> Healthcheck")
    conn = BaseHook.get_connection("openweathermap")
    ping('-c', 1, conn.host, _timeout=2)
    # return 'ping -c 1 -w 2 api.openweathermap.org'


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
    measurement = is_service_alive() >> scrape_data(query)
    line = process_data(measurement)
    publish_data(line)


if __name__ == "__main__":
    main().test()
else:
    main()
