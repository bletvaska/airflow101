#!/usr/bin/env python3

import json

import click
import httpx
from jsonschema import validate


def scrape_data(query: str, appid: str) -> dict:
    print(">> Scraping Data")
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'units': 'metric',
        'q': query,
        'appid': appid
    }
    response = httpx.get(url, params=params)
    data = response.json()
    return data


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


def publish_data(line: str):
    print(">> Publishing Data")

    # file = open('dataset.csv', mode='a')
    # print(line, file=file)
    # file.close()

    # with open('dataset.csv', mode='a') as dataset:
        # print(line, file=dataset)
    print(line)


def validate_schema(instance: dict) -> dict:
    with open('/home/ubuntu/airflow/dags/weather.schema.json', mode='r') as fp:
        schema = json.load(fp)

    validate(instance, schema)

    return instance

# @click.option('--query', default='kosice,sk', help='City name, state code and country code divided by comma.', show_default=True)
@click.command()
@click.option('--appid', default=None, envvar='APPID', help='Unique API key for openweathermap.org')
@click.argument('query')
def main(query: str, appid: str):
    json_data = scrape_data(query, appid)
    validated_data = validate_schema(json_data)
    entry = process_data(validated_data)
    publish_data(entry)


# [ scrape_data ] -> [validate_schema] -> [ process_data ] -> [ publish_data ]
# scrape_data | validate_schema | process_data | publish_data
if __name__ == '__main__':
    main()


#9e547051a2a00f2bf3e17a160063002d
