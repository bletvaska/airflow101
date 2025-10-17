#!/usr/bin/env python3

import httpx
import click



def scrape_data(appid: str, query: str, units: str) -> dict:
    """
    Scrapes data from a specified source.
    """
    print(">> Scraping Data")

    url = f"http://api.openweathermap.org/data/2.5/weather?appid={appid}&q={query}&units={units}"
    response = httpx.get(url)
    data = response.json()

    return data


def process_data(data: dict) -> str:
    """
    Processes the scraped data.
    """
    print(">> Processing Data")

    return "{};{};{};{};{};{};{};{};{};{};{}".format(
        data["main"]["temp"],
        data["main"]["humidity"],
        data["main"]["pressure"],
        data["name"],
        data["sys"]["country"],
        data["visibility"],
        data["wind"]["speed"],
        data["wind"]["deg"],
        data["sys"]["sunset"],
        data["sys"]["sunrise"],
        data["weather"][0]["description"],
    )


def publish_data(entry: str):
    """
    Publishes the processed data to a specified destination.
    """
    print(">> Publishing Data")

    with open("dataset.csv", "a") as dataset:
        print(entry, file=dataset)


@click.command(help="Download weather data and store it in a CSV file.")
@click.argument("query")
@click.option("--appid", default=None, help="API key for the weather service.")
@click.option(
    "--units",
    default="metric",
    help="Units for temperature.",
    type=click.Choice(["metric", "imperial", "standard"], case_sensitive=False),
)
def main(query: str, appid: str, units: str):
    data = scrape_data(appid, query, units)
    csv_entry = process_data(data)
    publish_data(csv_entry)


if __name__ == "__main__":
    main()
