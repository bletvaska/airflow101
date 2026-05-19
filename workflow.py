#!/usr/bin/env python3
import httpx
import click


def scrape_data(query: str, units: str, appid: str) -> dict:
    """
    Scrape weather data from the openweathermap.org.

    @return: A dictionary containing the scraped weather data as JSON (dictionary).
    """
    print(">> Scraping Data")

    url = 'https://api.openweathermap.org/data/2.5/weather'

    response = httpx.get(f'{url}?q={query}&appid={appid}&units={units}')

    if response.status_code == 401:
        print('Error "401 Unauthorized". Please check your API key and try again.')
        quit(1)

    elif response.status_code == 404:
        print(f'Error "404 Not Found". The city "{query}" was not found. Please check the city name and try again.')
        quit(1)

    if response.status_code != 200:
        print(f'Error "{response.status_code}" while fetching data from openweathermap.org')
        print(response.json()['message'])
        quit(1)

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


@click.option('--appid', '-a',
    help='API key for openweathermap.org. You can get it for free by creating an account on their website.',
    envvar='WORKFLOW_APPID',
    # required=True
)
@click.option('--units', '-u', 
    help='Units of measurement. standard, metric and imperial units are available.', 
    type=click.Choice(['standard', 'metric', 'imperial']), 
    default='metric'
)
@click.argument('query')
@click.command(help='Download current weather condition in CSV format.')
def main(query: str, units: str, appid: str):
    data = scrape_data(query, units, appid)
    csv_entry = process_data(data)
    publish_data(csv_entry)


if __name__ == "__main__":
    main()
