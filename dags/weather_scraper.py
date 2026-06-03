from datetime import datetime
from http import HTTPStatus
import logging

from airflow.sdk import dag, task
import httpx

DATASET_PATH = 'dataset.csv'

logger = logging.getLogger(__name__)


@task(task_display_name='Scrape Data')
def scrape_data(query: str, units: str, appid: str) -> dict:
    """
    Scrape weather data from the openweathermap.org.

    @return: A dictionary containing the scraped weather data as JSON (dictionary).
    """
    logger.info("Scraping Data")

    url = 'https://api.openweathermap.org/data/2.5/weather'

    response = httpx.get(f'{url}?q={query}&appid={appid}&units={units}')

    if response.status_code == HTTPStatus.UNAUTHORIZED:
        logger.error('Error "401 Unauthorized". Please check your API key and try again.')
        quit(1)

    elif response.status_code == HTTPStatus.NOT_FOUND:
        logger.error(f'Error "404 Not Found". The city "{query}" was not found. Please check the city name and try again.')
        quit(1)

    if response.status_code != HTTPStatus.OK:
        logger.error(f'Error "{response.status_code}" while fetching data from openweathermap.org')
        logger.error(response.json()['message'])
        quit(1)

    return response.json()


@task(task_display_name='Process Data')
def process_data(data: dict) -> str:
    """
    Process and transform the scraped weather data.

    @param data: A dictionary containing the scraped weather data as JSON (dictionary).
    @return: A string containing the processed weather data in CSV format.
    """
    logger.info("Processing Data")

    return "{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
        data['dt'],
        data['name'],
        data['sys']['country'],
        data['sys']['sunrise'],
        data['sys']['sunset'],
        data['main']['temp'],
        data['main']['temp_min'],
        data['main']['temp_max'],
        data['main']['humidity'],
        data['weather'][0]['description'],
        data['weather'][0]['main'],
        data['wind']['speed'],
        data['wind']['deg'],
    )


@task(task_display_name='Publish Data')
def publish_data(entry: str):
    """
    Publish the processed weather data as CSV file.

    @param entry: A string containing the processed weather data in CSV format.
    """
    logger.info("Publishing Data")

    with open(DATASET_PATH, 'a') as file:
        if file.tell() == 0:
            print('dt,name,country,sunrise,sunset,temp,temp_min,temp_max,humidity,description,main,wind_speed,wind_deg', file=file)
        print(entry, file=file)


@dag(
    'weather_scraper',
    dag_display_name='Weather Scraper',
    description='Scrapes weather data from openweathermap.org.',
    schedule='*/20 * * * *',
    start_date=datetime(2026, 6, 1),
    tags=['mirek', 'training', 'dt'],
    catchup=False
)
def main():
    data = scrape_data('kosice,sk', 'metric', '9e547051a2a00f2bf3e17a160063002d')
    csv_entry = process_data(data)
    publish_data(csv_entry)


main()
