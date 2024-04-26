import sys
from http import HTTPStatus

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import httpx
from pendulum import datetime


@task
def scrape_data(query):
    """
    Scrapes the data from openweathermap.org
    """
    print(">> Scraping Data")

    conn = BaseHook.get_connection("openweathermap")

    url = f"{conn.schema}://{conn.host}/data/2.5/weather"
    params = {"q": query, "appid": conn.password, "units": 'metric'}
    response = httpx.get(url, params=params)

    if response.status_code == HTTPStatus.NOT_FOUND:
        sys.exit("Error: City not found.")

    if response.status_code == HTTPStatus.UNAUTHORIZED:
        sys.exit("Error: Invalid API Key.")

    data = response.json()
    return data


@task
def process_data(data):
    """
    Process the passed data
    """
    print(">> Processing Data")

    return "{},{},{},{},{},{},{}".format(
        data["dt"],
        data["name"],
        data["main"]["temp"],
        data["main"]["pressure"],
        data["main"]["humidity"],
        data["wind"]["speed"],
        data["wind"]["deg"],
    )


@task
def publish_data(line):
    """
    Publish/persist the data to CSV file
    """
    print(">> Publishing Data")

    with open("dataset.csv", "a") as file:
        print(line, file=file)


@dag(
    "weather_scraper",
    description="Scrapes weather from openweathermap.org",
    schedule="*/20 * * * *",
    start_date=datetime(2024, 4, 1),
    tags=["weather", "devops", "t-sys", "tuke"],
    catchup=False,
)
def main(query: str = "kosice"):
    data = scrape_data(query)
    line = process_data(data)
    publish_data(line)


main()
