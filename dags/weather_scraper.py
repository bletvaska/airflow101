from http import HTTPStatus
import json

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
from pendulum import datetime


@task
def scrape_data(query: str, units: str) -> str:
    """
    Scrapes the data from openweathermap.org
    """
    conn = BaseHook.get_connection('openweathermap')

    base_url = f"{conn.schema}://{conn.host}:{conn.port}/data/2.5/weather"
    params = {
        'appid': conn.password,
        'q': query,
        'units': units,
    }
    response = httpx.get(base_url, params=params)

    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException("Error: Something is wrong.")
    
    return response.text


@task
def  process_data(data: str) -> str:
    """
    Process and extract the downloaded data.
    """
    # print('>> Processing Data')
    data = json.loads(data)
    result = "{};{};{};{};{};{};{};{};{};{}".format(
        data["dt"],
        data["name"],
        data["sys"]["country"],
        data["main"]["temp"],
        data["main"]["humidity"],
        data["main"]["pressure"],
        data["sys"]["sunrise"],
        data["sys"]["sunset"],
        data["wind"]["speed"],
        data["wind"]["deg"],
    )
    return result

    # return None


@task
def publish_data(line: str):
    """
    Data persistence.
    """
    # print('>> Publishing Data')

    with open("dataset.csv", "a") as dataset:
        print(line, file=dataset)


@dag(
    "weather_scraper",
    dag_display_name="Weather Scraper",
    description="Scrapes weather from openweathermap.org",
    schedule="*/20 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "devops", "dt"],
)
def main(query='kosice', units='metric'):
    measurement = scrape_data(query, units)
    entry = process_data(measurement)
    publish_data(entry)


if __name__ == '__main__':
    main().test()
else:
    main()
