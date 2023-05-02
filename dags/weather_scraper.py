from pendulum import datetime
from airflow import DAG
from airflow.decorators import task


with DAG(
    "weather_scraper",
    description="Weather scraper from openweathermap.org",
    schedule="*/20 * * * *",
    start_date=datetime(2023, 4, 29),
    catchup=False,
    tags=["training", "t-systems", "weather"],
):

    @task
    def get_name() -> str:
        print(">> get_name()")
        return 'jano'

    @task
    def greetings(name: str):
        print(">> greetings()")
        print(f'Hello {name}!')

    # get_name | greetings
    name = get_name()
    greetings(name)
