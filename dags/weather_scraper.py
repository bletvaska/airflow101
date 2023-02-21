# standard modules
from datetime import datetime
from pathlib import Path
import tempfile

# third party modules
from airflow import DAG
from airflow.decorators import task, dag
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
import httpx
import pendulum
from botocore.exceptions import ClientError

# my local modules
from helpers import get_minio_client
from tasks import is_minio_alive
from variables import BUCKET_DATASETS, DATASET_WEATHER


APPID = BaseHook.get_connection("openweathermap").password
BASE_URL = BaseHook.get_connection("openweathermap").host


@task
def is_service_alive() -> None:
    # get ready
    params = {"appid": APPID}

    try:
        # make request
        response = httpx.head(f"{BASE_URL}/data/2.5/weather", params=params)

        # if not 400, then stop DAG
        if response.status_code != 400:
            raise AirflowFailException("Invalid API key.")

    except httpx.ConnectError:
        raise AirflowFailException(f"Can't connect to host {BASE_URL}.")

    # return None


@task
def scrape_data() -> dict:
    # get ready
    params = {"q": "kosice,sk", "units": "metric", "appid": APPID}

    # scrape the data!
    response = httpx.get(f"{BASE_URL}/data/2.5/weather", params=params)

    # if http status code is not 200, then raise exception
    if response.status_code != 200:
        raise AirflowFailException(
            f"HTTP status code is not 200. Received value was {response.status_code}."
        )

    # forward data
    return response.json()


@task
def filter_data(data: dict) -> dict:
    # result = {}

    # result['dt'] = data['dt']
    # result['city'] = data['name']
    # result['country'] = data['sys']['country']
    # result['temperature'] = data['main']['temp']
    # result['humidity'] = data['main']['humidity']
    # result['pressure'] = data['main']['pressure']
    # result['wind_speed'] = data['wind']['speed']
    # result['wind_deg'] = data['wind']['deg']
    # result['sunrise'] = data['sys']['sunrise']
    # result['sunset'] = data['sys']['sunset']

    result = {
        "dt": data["dt"],
        "city": data["name"],
        "country": data["sys"]["country"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
        "wind_speed": data["wind"]["speed"],
        "wind_deg": data["wind"]["deg"],
        "sunrise": data["sys"]["sunrise"],
        "sunset": data["sys"]["sunset"],
    }

    return result


@task
def process_data(data: dict) -> None:
    minio = get_minio_client()

    # get bucket
    bucket = minio.Bucket(BUCKET_DATASETS)

    # create temporary file
    path = Path(tempfile.mkstemp()[1])  # Path("/home/ubuntu/weather.csv")

    # download weather.csv from S3/MinIO
    try:
        bucket.download_file(Key=DATASET_WEATHER, Filename=path)
    except ClientError:
        print("Given dataset was not found on S3/MinIO.")

        # if no CSV file yet, then create header
        with open(path, "w") as file:
            print(
                "dt,city,country,temperature,humidity,pressure,sunrise,sunset,wind_speed,wind_deg",
                file=file,
            )

    # append data
    with open(path, "a") as file:
        print(
            "{},{},{},{},{},{},{},{},{},{}".format(
                pendulum.from_timestamp(data["dt"]).to_iso8601_string(),
                data["city"],
                data["country"],
                data["temperature"],
                data["humidity"],
                data["pressure"],
                pendulum.from_timestamp(data["sunrise"]).to_iso8601_string(),
                pendulum.from_timestamp(data["sunset"]).to_iso8601_string(),
                data["wind_speed"],
                data["wind_deg"],
            ),
            file=file,
        )

    # upload data
    bucket.upload_file(Filename=path, Key=DATASET_WEATHER)

    # delete file
    path.unlink(missing_ok=True)

    return data


# @task
# def publish_data(report) -> None:
#     pass


with DAG(
    dag_id="weather_scraper",
    catchup=False,
    description="Scrapes weather from openweathermap.org",
    start_date=datetime(2023, 1, 23),
    schedule="*/15 * * * *",
):
    # from IPython import embed; embed()

    # scrape_data() >> process_data() >> publish_data()
    # scrape_data | process_data | publish_data

    data = [is_minio_alive(), is_service_alive()] >> scrape_data()
    filtered_data = filter_data(data)
    report = process_data(filtered_data)
    # publish_data(report)

