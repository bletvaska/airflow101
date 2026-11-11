import logging
from http import HTTPStatus
from pathlib import Path

import httpx
from airflow.sdk import BaseHook, dag, task
from airflow.sdk.exceptions import AirflowFailException
from pendulum import datetime, from_timestamp

logger = logging.getLogger(__name__)


@task(task_display_name="Scrape Data")
def scrape_data(query: str) -> dict:
    """
    Scrapes data from openweathermap.org
    """
    conn = BaseHook.get_connection("openweathermap")

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


@dag(
    "weather_scraper",
    dag_display_name="Weather Scraper 20m",
    description="Scrapes weather from openweathermap.org",
    schedule="*/20 * * * *",
    start_date=datetime(2026, 10, 27),
    end_date=datetime(2026, 12, 24),
    tags=["weather", "training", "dt"],
    catchup=False,
)
def main(query: str = "kosice"):
    measurement = scrape_data(query)
    entry = process_data(measurement)
    publish_data(entry)


#    CTRL+Space - intellisence

if __name__ == "__main__":
    main().test()
else:
    main()
