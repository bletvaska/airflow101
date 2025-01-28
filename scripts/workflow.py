#!/usr/bin/env python

import json

import httpx
import click

# appid='9e547051a2a00f2bf3e17a160063002d'


def scrape_data(query: str, units: str, appid: str) -> str:
    """
    Scrapes the data from openweathermap.org
    """
    # print('>> Scraping Data')
    base_url = "https://api.openweathermap.org/data/2.5/weather"

    url = f"{base_url}?appid={appid}&q={query}&units={units}"
    response = httpx.get(url)
    return response.text


def processing_data(data: str) -> str:
    """
    Process and extract the downloaded data.
    """
    # print('>> Processing Data')
    data = json.loads(data)
    result = "{};{};{};{};{};{};{};{};{};{}".format(
        data["dt"],
        data["name"],
        data["sys"]["country"],
        data["main"]["temp"],
        data["main"]["humidity"],
        data["main"]["pressure"],
        data["sys"]["sunrise"],
        data["sys"]["sunset"],
        data["wind"]["speed"],
        data["wind"]["deg"],
    )
    return result

    # return None


def publish_data(line: str):
    """
    Data persistence.
    """
    # print('>> Publishing Data')

    with open("dataset.csv", "a") as dataset:
        print(line, file=dataset)


@click.command(help="Download current weather condition in CSV format.")
@click.option(
    "--units",
    help="Unit of measurement",
    type=click.Choice(["metric", "standard", "imperial"]),
    default="metric",
)
@click.option("--appid", help="Unique API key for openweathermap.org", envvar="APPID")
@click.argument("query")
def main(query: str, units: str, appid: str):
    # scrape_data | process_data | publish_data
    measurement = scrape_data(query, units, appid)
    entry = processing_data(measurement)
    publish_data(entry)


if __name__ == "__main__":
    main()
