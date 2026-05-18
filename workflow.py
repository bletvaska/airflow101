#!/usr/bin/env python3
import httpx


def scrape_data() -> dict:
    """
    Scrape weather data from the openweathermap.org.

    @return: A dictionary containing the scraped weather data as JSON (dictionary).
    """
    print(">> Scraping Data")

    url = 'https://api.openweathermap.org/data/2.5/weather'
    query = 'kosice'
    appid = '9e547051a2a00f2bf3e17a160063002d'
    units = 'metric'

    response = httpx.get(f'{url}?q={query}&appid={appid}&units={units}')

    return response.json()


def process_data(data: dict) -> str:
    """
    Process and transform the scraped weather data.

    @param data: A dictionary containing the scraped weather data as JSON (dictionary).
    @return: A string containing the processed weather data in CSV format.
    """
    print(">> Processing Data")

    return "{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
        data['dt'],
        data['name'],
        data['sys']['country'],
        data['sys']['sunrise'],
        data['sys']['sunset'],
        data['main']['temp'],
        data['main']['temp_min'],
        data['main']['temp_max'],
        data['main']['humidity'],
        data['weather'][0]['description'],
        data['weather'][0]['main'],
        data['wind']['speed'],
        data['wind']['deg'],
    )


def publish_data(entry: str):
    """
    Publish the processed weather data as CSV file.

    @param entry: A string containing the processed weather data in CSV format.
    """
    print(">> Publishing Data")
    print(entry)


def main():
    data = scrape_data()
    csv_entry = process_data(data)
    publish_data(csv_entry)


if __name__ == "__main__":
    main()
