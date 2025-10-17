import json
from pathlib import Path
import logging

from airflow.sdk import dag, task, BaseHook
from pendulum import datetime
import httpx
import jsonschema

logger = logging.getLogger(__name__)


@task(task_display_name="Scrape Data")
def scrape_data(query: str, units: str) -> dict:
    """
    Scrapes data from a specified source.
    """
    logger.info("Scraping Data")
    logger.debug('toto je debug')
    logger.warning('toto je warning')
    logger.error('toto je error')
    logger.critical('toto je critical')

    conn = BaseHook.get_connection("openweathermap")

    url = f"{conn.schema}://{conn.host}:{conn.port}"
    path = "data/2.5/weather"
    params = f"appid={conn.password}&q={query}&units={units}"

    response = httpx.get(f"{url}/{path}?{params}")
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

    path = Path(__file__).parent.parent / "dataset.csv"
    with open(path, "a") as dataset:
        print(entry, file=dataset)


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
    data = scrape_data(query, units)
    validated_data = validate_data(data)
    csv_entry = process_data(validated_data)
    publish_data(csv_entry)


if __name__ == "__main__":
    main().test()
else:
    main()
