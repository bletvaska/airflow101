import httpx
from pendulum import datetime
from airflow import DAG
from airflow.decorators import task


with DAG(
    "weather_scraper",
    description="Scrapes weather from openweathermap.org service.",
    schedule="*/20 * * * *",
    start_date=datetime(2023, 4, 29),
    catchup=False,
    tags=["training", "t-systems", "weather"],
):

    @task
    def download_data() -> dict:
        print(">> downloading data")

        # get ready
        query = "kosice"
        api_key = "9e547051a2a00f2bf3e17a160063002d"
        url = (
            f"https://api.openweathermap.org/data/2.5/weather?q={query}&appid={api_key}"
        )

        # scrape data
        response = httpx.get(url)

        return response.json()

    @task
    def process_data(data: dict) -> str:
        print(">> processing data")

        # dt; main.temp; main.humidity; main.pressure; weather.main; visibility; wind.speed; wind.deg;
        return "{};{};{};{};{};{};{};{}".format(
            data["dt"],
            data["main"]["temp"],
            data["main"]["humidity"],
            data["main"]["pressure"],
            data["weather"][0]["main"],
            data["visibility"],
            data["wind"]["speed"],
            data["wind"]["deg"],
        )

    @task
    def upload_data(entry: str):
        print(">> uploading data")

        with open("dataset.csv", mode="a") as file:
            file.write(f"{entry}\n")

    data = download_data()
    entry = process_data(data)
    upload_data(entry)
