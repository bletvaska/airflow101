#!/usr/bin/env python

import click
import httpx
import pendulum


def scrape_data(query: str, appid: str, units: str) -> dict:
    """
    Scrapes data from openweathermap.org
    """
    print(">> Scraping Data")

    # query = 'kosice'
    # appid = '9e547051a2a00f2bf3e17a160063002d'
    # units = 'metric'

    # SELECT * FROM weather
    # WHERE
    # q = 'kosice'
    # AND appid = '9e547051a2a00f2bf3e17a160063002d'
    # AND units = 'metric'
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = f"q={query}&appid={appid}&units={units}"

    response = httpx.get(f"{url}?{params}")

    return response.json()


def process_data(data: dict) -> dict:
    """
    Process the downloaded data
    """
    print(">> Processing Data")

    result = {
        "dt": pendulum.from_timestamp(data["dt"], "UTC").to_iso8601_string(),
        "city": data["name"],
        "country": data["sys"]["country"],
        "weather": data["weather"][0]["id"],
        "temp": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
    }

    return result


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

    with open("dataset.csv", mode="a") as file:
        print(line, file=file)

    # {'dt': '2026-10-28T08:24:48Z', 'city': 'Kosice', 'country': 'SK', 'weather': 800, 'temp':10.14, 'humidity': 71, 'pressure': 1022}
    # 2026-10-28T08:24:48Z,Kosice,SK,800,10.14,


@click.command(help="Download current weather conditions in CSV format.")
@click.argument("query")
@click.option("--appid", "-a", help="Unique API key for openweathermap.org")
@click.option("--units", "-u", help="Unit of measurement", 
              type=click.Choice(['metric', 'standard', 'imperial']), default='metric')
def main(query: str, appid: str, units: str):
    measurement = scrape_data(query, appid, units)
    entry = process_data(measurement)
    publish_data(entry)


if __name__ == "__main__":
    main()
