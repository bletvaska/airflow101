#!/usr/bin/env python

import httpx
import pendulum


def scrape_data() -> dict:
    """
    Scrapes data from openweathermap.org
    """
    print(">> Scraping Data")
    
    query = 'kosice'
    appid = '9e547051a2a00f2bf3e17a160063002d'
    units = 'metric'

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = f"q={query}&appid={appid}&units={units}"

    response = httpx.get(f'{url}?{params}')

    return response.json()


def process_data(data: dict) -> dict:
    """
    Process the downloaded data
    """
    print(">> Processing Data")

    result = {
        'dt': pendulum.from_timestamp(data['dt'], 'UTC').to_iso8601_string(),
        'city': data['name'],
        'country': data['sys']['country'],
        'weather': data['weather'][0]['id'],
        'temp': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
    }

    return result


def publish_data(data: dict):
    """
    Data persistence
    """
    print(">> Publishing Data")

    file = open('dataset.csv', mode='a')
    print('kurnik sopa', file=file)
    file.close()


def main():
    measurement = scrape_data()
    entry = process_data(measurement)
    publish_data(entry)


if __name__ == "__main__":
    main()
