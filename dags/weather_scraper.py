import json
from datetime import datetime
from pathlib import Path
import urllib3

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import requests
import jsonschema
from pydantic import validator
from sqlmodel import Field, SQLModel, create_engine, Session
from minio import Minio

CONNECTION_ID = "openweathermap"


class Measurement(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    temperature: float
    humidity: int
    pressure: int
    city: str
    country: str
    dt: datetime

    @validator("humidity")
    def humidity_must_be_in_range_from_zero_to_hundred(cls, value):
        if 0 <= value <= 100:
            return value

        raise ValueError("must be in range [0, 100]")

    @validator("temperature")
    def temperature_must_be_greater_than_absolute_zero(cls, value):
        if value < -273:
            raise ValueError("must be greater than -273")

        return value

    @validator("country")
    def coutry_must_be_specific(cls, value):
        if len(value) != 2:
            raise ValueError("length of country code must be 2")

        elif value not in ["SK", "CZ", "HU", "PL", "UA", "AU"]:
            raise ValueError("country must be one of specific")

        else:
            return value

    def csv(self):
        return f"{self.dt};{self.country};{self.city};{self.temperature};{self.humidity};{self.pressure}"


with DAG(
    "weather_scraper",
    description="Scrapes and processes the weather data.",
    schedule="*/15 * * * *",
    start_date=datetime(2022, 9, 28),
    catchup=False,
):


    @task
    def scrape_weather_data() -> dict:
        """
        Scrapes weather data from openweathermap.org.

        Function returns the data returned from the service as dictionary.
        """
        # prepare for query
        connection = BaseHook.get_connection(CONNECTION_ID)
        params = {
            "q": Variable.get("openweathermap_query", "poprad,sk"),
            "appid": connection.password,
            "units": "metric",
        }

        # request weather info
        with requests.get(
            f"{connection.host}{connection.schema}", params=params
        ) as response:
            data = response.json()
            return data

            # request.close()


    @task
    def is_jsondb_valid():
        """
        Checks, if weather.json exists and is valid.
        """
        path = Path(__file__).parent / "weather.json"

        try:
            with open(path, "r+") as file:
                json.load(file)

        except (json.decoder.JSONDecodeError, FileNotFoundError):
            with open(path, "w") as file:
                print("{}", file=file)


    @task
    def save_to_jsondb(payload: dict):
        """
        Saves the latest measurement to wather.json.

        The `payload` parameter contains latest filtered measurement.
        """
        path = Path(__file__).parent / "weather.json"

        # load existing records
        with open(path, "r") as file:
            db = json.load(file)

        # add new entry
        db[payload["dt"]] = payload

        # save new one
        with open(path, "w") as file:
            print(json.dumps(db), file=file)


    @task
    def is_service_alive():
        """
        Checks, if the service is available and credentials are valid.

        If service is not available or credentials (app token) are not valid, the `AirflowFailException` is raised.
        """
        # create request url
        connection = BaseHook.get_connection(CONNECTION_ID)
        query = Variable.get("openweathermap_query", "poprad,sk")
        url = f"{connection.host}{connection.schema}?q={query}&appid={connection.password}"

        try:
            # http request
            with requests.get(url) as response:

                # if http status code is not 200, then stop workflow
                if response.status_code != 200:
                    data = response.json()
                    raise AirflowFailException(
                        f'{response.status_code}: {data["message"]}'
                    )

                # request.close()

        # if hostname doesn't exist, then stop workflow
        except requests.exceptions.ConnectionError:
            raise AirflowFailException("Openweathermap.org is not available.")


    @task
    def filter_data(raw_data: dict) -> dict:
        """
        Returns only the data we are interested in.

        Parameter `raw_data` contains the data returned from the service openweathermap.org.
        """
        # filter data
        data = {
            "temperature": raw_data["main"]["temp"],
            "humidity": raw_data["main"]["humidity"],
            "pressure": raw_data["main"]["pressure"],
            "city": raw_data["name"],
            "country": raw_data["sys"]["country"],
            "dt": raw_data["dt"],
        }

        return data


    @task
    def validate_data(payload: dict) -> dict:
        """
        Validates filtered data according JSON Schema.

        The `payload` parameter contains latest filtered measurement.
        """
        # create absolute path to schema.json
        path = Path(__file__).parent / "schema.json"

        # json schema validation
        with open(path, "r") as file:
            schema = json.load(file)
            jsonschema.validate(instance=payload, schema=schema)
            return payload

            # file.close()


    @task
    def save_to_csv(payload: dict):
        """
        Appends the latest measurement to CSV file.

        The `payload` parameter contains latest filtered measurement.
        """
        # create absolute path to data.csv
        path = Path(__file__).parent / "weather.csv"
        data = Measurement(**payload)

        with open(path, "a") as file:
            # print(f'{data.dt};{data.country};{data.city};{data.temperature};{data.humidity};{data.pressure};', file=file)
            # print(data.dt, data.country, data.city, data.temperature, data.humidity, data.pressure, sep=';', file=file)
            print(data.csv(), file=file)


    @task
    def is_minio_alive():
        """
        Checks if minio and bucket "datasets" are available.

        If the bucket doesn't exist, the function will create it.
        """
        try:
            # create MinIO client
            client = Minio('minio:9000', 'admin', 'administrator', secure=False)

            # check if bucket exists
            if client.bucket_exists('datasets') == False:
                client.make_bucket('datasets')
        except urllib3.exceptions.MaxRetryError:
            raise AirflowFailException(f'MinIO not available.')


    @task
    def upload_to_minio():
        """
        Uploads CSV dataset to MinIO/S3 bucket.
        """
        # create MinIO client
        client = Minio('minio:9000', 'admin', 'administrator', secure=False)

        # upload object/file
        client.fput_object(
            "datasets",
            "weather.csv",
            Path(__file__).parent / "weather.csv"
        )


    @task
    def save_to_db(payload: dict):
        """
        Inserts the latest measurement to the SQL database.

        The `payload` parameter contains latest filtered measurement.
        """
        conn = BaseHook.get_connection("openweathermap_db")

        engine = create_engine(f"{conn.schema}://{conn.host}")
        SQLModel.metadata.create_all(engine)

        data = Measurement(**payload)

        with Session(engine) as session:
            # INSERT INTO measurement VALUES()
            session.add(data)
            session.commit()

    #
    raw_data = is_service_alive() >> scrape_weather_data()
    filtered_data = filter_data(raw_data)
    validated_data = validate_data(filtered_data)
    is_jsondb_valid() >> save_to_jsondb(validated_data)
    save_to_csv(filtered_data) >> is_minio_alive() >> upload_to_minio()
    save_to_db(filtered_data)
