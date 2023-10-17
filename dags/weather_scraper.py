import json

from pendulum import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.hooks.base import BaseHook
import httpx
from jsonschema import validate


@task
def scrape_data(query: str) -> dict:
    print(">> Scraping Data")
    url = "http://api.openweathermap.org/data/2.5/weather"
    connection = BaseHook.get_connection('openweathermap')
    params = {
        'units': 'metric',
        'q': query,
        'appid': connection.password
    }
    response = httpx.get(url, params=params)
    data = response.json()
    return data


@task
def process_data(data: dict) -> str:
    print(">> Processing Data")

    # return f'{data["dt"]};' \
    #        f'mesto;krajina;teplota;tlak;vlhkost;sila vetra; smer vetra;vychod slnka;zapad slnka'

    return '{};{};{};{};{};{};{};{};{};{}'.format(
        data['dt'],                 # datetime
        data['name'],               # mesto
        data['sys']['country'],     # krajina
        data['main']['temp'],       # teplota
        data['main']['pressure'],   # tlak
        data['main']['humidity'],   # vlhkost
        data['wind']['speed'],      # vietor - rychlost
        data['wind']['deg'],        # vietor - smer,
        data['sys']['sunrise'],     # vychod slnka
        data['sys']['sunset'],      # zapad slnka
    )


@task
def publish_data(line: str):
    print(">> Publishing Data")

    # file = open('dataset.csv', mode='a')
    # print(line, file=file)
    # file.close()

    with open('dataset.csv', mode='a') as dataset:
        print(line, file=dataset)


@task
def validate_schema(instance: dict) -> dict:
    with open('/home/ubuntu/airflow/dags/weather.schema.json', mode='r') as fp:
        schema = json.load(fp)

    validate(instance, schema)

    return instance


# [ scrape_data ] -> [validate_schema] -> [ process_data ] -> [ publish_data ]
# scrape_data | validate_schema | process_data | publish_data
@dag(
    'weather_scraper',
    start_date=datetime(2023, 10, 1),
    schedule='*/20 * * * *',
    description='Scrapes weather from openweathermap.org',
    catchup=False,
)
def main(query=Param(type='string', default='kosice,sk', title='Query', description='Name of the city to get wather info.')):
    json_data = scrape_data('kosice,sk')
    validated_data = validate_schema(json_data)
    entry = process_data(validated_data)
    publish_data(entry)


main()
