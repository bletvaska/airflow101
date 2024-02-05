import logging

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.exceptions import AirflowFailException
import httpx
from pendulum import datetime

logger = logging.getLogger(__file__)

@task
def scrape_data(query: str) -> dict:
    logger.info('Scraping data')

    connection = BaseHook.get_connection('openweathermap')

    params = {
        'q': query,
        'units': 'metric',
        'appid': connection.get_password()
    }

    response = httpx.get(connection.host, params=params)
    return response.json()


@task
def process_data(data: dict) -> str:
    logger.info('Processing data')

    line = '{},{},{},{},{},{},{},{},{},{}'.format(
        data['dt'],
        data['name'],
        data['sys']['country'],
        data['main']['temp'],
        data['main']['humidity'],
        data['main']['pressure'],
        data['sys']['sunrise'],
        data['sys']['sunset'],
        data['wind']['deg'],
        data['wind']['speed'],
    )

    return line


@task
def publish_data(line: str):
    logger.info('Publishing data')

    with open('/home/ubuntu/dataset.csv', 'a') as dataset:
        print(line, file=dataset)


@task
def is_service_alive():
    try:
        connection = BaseHook.get_connection('openweathermap')
        params = {
            'appid': connection.get_password()
        }
        response = httpx.get(connection.host, params=params)
        if response.status_code == 401:
            raise AirflowFailException('Invalid API Key')
    except httpx.ConnectError:
        logger.error(f'Invalid hostname {connection.host}')
        raise AirflowFailException('Invalid host name.')


@dag(
    'weather_scraper',
    description='Scrapes weather from openweathermap.org',
    schedule='*/20 * * * *',
    start_date=datetime(2024, 1, 1, tz='UTC'),
    tags=['weather', 'devops', 'telekom'],
    catchup=False
)
def main(query=Param(type='string', default='kosice,sk', title='Query', description='Name of the city to get weather info about')):
    # is_service_alive | scrape_data | process_data | publish_data
    data = is_service_alive() >> scrape_data('kosice,sk')
    processed_data = process_data(data)
    publish_data(processed_data)

main()
