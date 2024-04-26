#!/usr/bin/env python3

import httpx
import click
import sys
from http import HTTPStatus


def scrape_data(query, appid, units):
    """
    Scrapes the data from openweathermap.org
    """
    print(">> Scraping Data")

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": query, "appid": appid, "units": units}
    response = httpx.get(url, params=params)

    if response.status_code == HTTPStatus.NOT_FOUND:
        sys.exit("Error: City not found.")

    if response.status_code == HTTPStatus.UNAUTHORIZED:
        sys.exit("Error: Invalid API Key.")

    data = response.json()
    return data


def process_data(data):
    """
    Process the passed data
    """
    print(">> Processing Data")

    return "{},{},{},{},{},{},{}".format(
        data["dt"],
        data["name"],
        data["main"]["temp"],
        data["main"]["pressure"],
        data["main"]["humidity"],
        data["wind"]["speed"],
        data["wind"]["deg"],
    )


def publish_data(line):
    """
    Publish/persist the data to CSV file
    """
    print(">> Publishing Data")

    with open("dataset.csv", "a") as file:
        print(line, file=file)


# run the workflow
# scrape_data | process_data | publish_data
@click.command(help="Download current weather condition in CSV format.")
@click.option(
    "--appid",
    help="Unique API key for openweathermap.org",
    default=None,
    envvar="APPID",
)
@click.option(
    "--units",
    help="Unit of measurement.",
    type=click.Choice(["metric", "standard", "imperial"]),
    default="metric",
)
@click.argument("query")
def main(query: str, appid: str, units: str):
    data = scrape_data(query, appid, units)
    line = process_data(data)
    publish_data(line)


if __name__ == "__main__":
    main()
