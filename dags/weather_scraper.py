from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
import httpx
from pendulum import datetime


@task
def scrape_data(query: str) -> dict:
    print('>> Scraping data')
    # print(query)

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
    print('>> Processing data')

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
    print('>> Publishing data')

    with open('/home/ubuntu/dataset.csv', 'a') as dataset:
        print(line, file=dataset)

@dag(
    'weather_scraper',
    description='Scrapes weather from openweathermap.org',
    schedule='*/20 * * * *',
    start_date=datetime(2024, 1, 1, tz='UTC'),
    tags=['weather', 'devops', 'telekom'],
    catchup=False
)
def main(query=Param(type='string', default='kosice,sk', title='Query', description='Name of the city to get weather info about')):
    data = scrape_data(query)
    processed_data = process_data(data)
    publish_data(processed_data)

main()
