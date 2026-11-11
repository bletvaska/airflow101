from pathlib import Path

import httpx
from airflow.sdk import dag, task
from pendulum import datetime, from_timestamp


@task(task_display_name="Scrape Data")
def scrape_data(query: str, appid: str, units: str) -> dict:
    """
    Scrapes data from openweathermap.org
    """
    print(">> Scraping Data")

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = f"q={query}&appid={appid}&units={units}"

    response = httpx.get(f"{url}?{params}")

    return response.json()


@task(task_display_name="Process Data")
def process_data(data: dict) -> dict:
    """
    Process the downloaded data
    """
    print(">> Processing Data")

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
    print(">> Publishing Data")

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
    path = Path(__file__).parent.parent / 'datasets'
    path.mkdir(parents=True, exist_ok=True)  # mkdir /path/to/folder --parents

    # append new line to dataset file
    with open(path / 'dataset.csv', mode="a") as dataset:
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
def main():
    measurement = scrape_data("kosice", "9e547051a2a00f2bf3e17a160063002d", "metric")
    entry = process_data(measurement)
    publish_data(entry)


#    CTRL+Space - intellisence


main()
