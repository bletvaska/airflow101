#!/usr/bin/env python


import click
import httpx


def scrape_data(query: str, units: str, appid: str) -> dict:
    print('>> Scraping data')

    url = 'http://api.openweathermap.org/data/2.5/weather'
    params = {
        'q': query,
        'units': units,
        'appid': appid
    }

    response = httpx.get(url, params=params)
    return response.json()


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


def publish_data(line: str):
    print('>> Publishing data')

    with open('/home/ubuntu/dataset.csv', 'a') as dataset:
        print(line, file=dataset)


@click.command(help='Download current weather condition in CSV format.')
@click.argument('query')
@click.option('--units', type=click.Choice(['metric', 'standard', 'imperial']), default='metric', help='Unit of measurement')
@click.option('--appid', default=None, help='API keZy for openweathermap.org service.', envvar='APPID')
def main(query: str, units: str, appid: str):
    # print(query, units, appid)
    data = scrape_data(query, units, appid)
    processed_data = process_data(data)
    publish_data(processed_data)


main()
