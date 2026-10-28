import httpx
from airflow.sdk import dag, task
from pendulum import datetime


@task(task_display_name='Scrape Data')
def scrape_data(query: str, appid: str, units: str) -> dict:
    """
    Scrapes data from openweathermap.org
    """
    print(">> Scraping Data")

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = f"q={query}&appid={appid}&units={units}"

    response = httpx.get(f"{url}?{params}")

    return response.json()


@dag(
    'weather_scraper',
    dag_display_name='Weather Scraper 20m',
    description='Scrapes weather from openweathermap.org',
    schedule='*/20 * * * *',
    start_date=datetime(2026, 10, 27),
    end_date=datetime(2026, 12, 24),
    tags=['weather', 'training', 'dt'],
    catchup=False,
)
def main():
    scrape_data('kosice', '9e547051a2a00f2bf3e17a160063002d', 'metric')

main()
