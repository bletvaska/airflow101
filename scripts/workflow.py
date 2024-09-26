#!/usr/bin/env python3
import httpx
import click


def scrape_data(query: str, appid: str, units: str) -> dict:
    """
    Scrapes data from external source.
    """
    url = f"https://api.openweathermap.org/data/2.5/weather?appid={appid}&q={query}&units={units}"

    print(">> Scraping Data")
    response = httpx.get(url)
    data = response.json()
    return data


def process_data(data: dict) -> str:
    """
    Processes and filters measurement data.
    """
    print(">> Processing Data")
    result = "{},{},{},{},{},{},{},{}".format(
        data["dt"],
        data["sys"]["country"],
        data["name"],
        data["main"]["temp"],
        data["main"]["humidity"],
        data["main"]["pressure"],
        data["wind"]["speed"],
        data["wind"]["deg"],
    )
    return result


def publish_data(line: str):
    """
    Saves measurement to CSV file.
    """
    print(">> Publishing Data")
    with open("dataset.csv", mode="a") as dataset:
        print(line, file=dataset)


@click.command(help="Weather downloader.")
@click.argument("query")
@click.option(
    "--appid",
    default=None,
    envvar="APPID",
    help="Unique API key for openweathermap.org",
)
@click.option(
    "--units",
    help="Unit of measurement",
    type=click.Choice(["metric", "standard", "imperial"]),
    default="metric",
)
def main(query: str, appid: str, units: str):
    # scrape_data | process_data | publish_data
    measurement = scrape_data(query, appid, units)
    line = process_data(measurement)
    publish_data(line)


main()
