import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import requests
import jsonschema
from pydantic import BaseModel, validator
from sqlmodel import Field, SQLModel

CONNECTION_ID = 'openweathermap'

class Measurement(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    temperature: float
    humidity: int
    pressure: int
    city: str
    country: str
    dt: datetime

    @validator('humidity')
    def humidity_must_be_in_range_from_zero_to_hundred(cls, value):
        if 0 <= value <= 100:
            return value

        raise ValueError('must be in range [0, 100]')

    @validator('temperature')
    def temperature_must_be_greater_than_absolute_zero(cls, value):
        if value < -273:
            raise ValueError('must be greater than -273')

        return value

    @validator('country')
    def coutry_must_be_specific(cls, value):
        if len(value) != 2:
            raise ValueError('length of country code must be 2')

        elif value not in ['SK', 'CZ', 'HU', 'PL', 'UA', 'AU']:
            raise ValueError('country must be one of specific')

        else:
            return value

    def csv(self):
        return f'{self.dt};{self.country};{self.city};{self.temperature};{self.humidity};{self.pressure}'



with DAG("weather_scraper",
   description="Scrapes and processes the weather data.",
   schedule="*/15 * * * *",
   start_date=datetime(2022, 9, 28),
   catchup=False):

    @task
    def scrape_weather_data():
        # prepare for query
        connection = BaseHook.get_connection(CONNECTION_ID)
        params = {
            'q': Variable.get('openweathermap_query', 'poprad,sk'),
            'appid': connection.password,
            'units': 'metric'
        }

        # request weather info
        with requests.get(f'{connection.host}{connection.schema}', params=params) as response:
            data = response.json()
            return data

            # request.close()

    @task
    def is_jsondb_valid():
        path = Path(__file__).parent / 'weather.json'

        try:
            with open(path, 'r+') as file:
                json.load(file)

        except (json.decoder.JSONDecodeError, FileNotFoundError):
            with open(path, 'w') as file:
                print('{}', file=file)



    @task
    def save_to_jsondb(data: dict):
        print('>>> processing data')
        path = Path(__file__).parent / 'weather.json'

        # load existing records
        with open(path, 'r') as file:
            db = json.load(file)

        # add new entry
        db[data['dt']] = data

        # save new one
        with open(path, 'w') as file:
            print(json.dumps(db), file=file)





        print(data)

    @task
    def publish_data():
        print('>>> publishing data')

    @task
    def is_service_alive():
        # create request url
        connection = BaseHook.get_connection(CONNECTION_ID)
        query = Variable.get('openweathermap_query', 'poprad,sk')
        url = f'{connection.host}{connection.schema}?q={query}&appid={connection.password}'

        try:
            # http request
            with requests.get(url) as response:

                # if http status code is not 200, then stop workflow
                if response.status_code != 200:
                    data = response.json()
                    raise AirflowFailException(f'{response.status_code}: {data["message"]}')

                # request.close()

        # if hostname doesn't exist, then stop workflow
        except requests.exceptions.ConnectionError:
            raise AirflowFailException('Openweathermap.org is not available.')

    @task
    def filter_data(payload: dict):
        # filter data
        data = {
            'temperature': payload['main']['temp'],
            'humidity': payload['main']['humidity'],
            'pressure': payload['main']['pressure'],
            'city': payload['name'],
            'country': payload['sys']['country'],
            'dt': payload['dt']
        }

        return data

    @task
    def validate_data(payload: dict):
        # create absolute path to schema.json
        path = Path(__file__).parent / 'schema.json'

        # json schema validation
        with open(path, 'r') as file:
            schema = json.load(file)
            jsonschema.validate(instance=payload, schema=schema)
            return payload

            # file.close()


    @task
    def save_to_csv(payload: dict):
        # create absolute path to data.csv
        path = Path(__file__).parent / 'weather.csv'
        data = Measurement(**payload)

        with open(path, 'a') as file:
            # print(f'{data.dt};{data.country};{data.city};{data.temperature};{data.humidity};{data.pressure};', file=file)
            # print(data.dt, data.country, data.city, data.temperature, data.humidity, data.pressure, sep=';', file=file)
            print(data.csv(), file=file)

    #
    raw_data = is_service_alive() >> scrape_weather_data()
    filtered_data = filter_data(raw_data)
    validated_data = validate_data(filtered_data)
    is_jsondb_valid() >> save_to_jsondb(validated_data) >> publish_data()
    save_to_csv(filtered_data)
