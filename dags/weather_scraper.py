from datetime import datetime

from airflow import DAG
from airflow.models import Variable, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from pydantic import BaseModel


class Measurement(BaseModel):
    dt: int
    temp: float
    pressure: int
    humidity: int
    wind: float
    country: str
    city: str


def _filter_data(ti: TaskInstance):
    payload = ti.xcom_pull(task_ids=['scrape_weather_data'])[0]

    # prepare data
    data = payload['main']
    data['dt'] = payload['dt']
    data['wind'] = payload['wind']['speed']
    data['country'] = payload['sys']['country']
    data['city'] = payload['name']

    # create measurement from data
    measurement = Measurement(**data)

    print(measurement)


with DAG('openweathermap_scraper',
         description='Scrapes the weather data from Openweathermap.org',
         schedule_interval='*/15 * * * *',
         start_date=datetime(2022, 3, 18)) as dag:
    task2 = DummyOperator(task_id='process_data')
    task3 = DummyOperator(task_id='store_data')

    scrape_data = SimpleHttpOperator(
        task_id='scrape_weather_data',
        method='GET',
        http_conn_id='openweathermap_api',
        endpoint='/data/2.5/weather',
        data={
            'q': Variable.get('openweathermap_query', default_var='kosice,sk'),
            'units': 'metric',
            'appid': Variable.get('openweathermap_appid')
        },
        response_filter=lambda response: response.json(),
        log_response=True
    )

    service_availability = HttpSensor(
        task_id='is_weather_api_available',
        http_conn_id='openweathermap_api',
        endpoint='/data/2.5/weather',
        request_params={
            'appid': Variable.get('openweathermap_appid')
        },
        poke_interval=10,
        timeout=30,
        method='GET',
    )

    data_preprocessor = PythonOperator(
        task_id='preprocess_data',
        python_callable=_filter_data
    )

    service_availability >> scrape_data >> data_preprocessor >> task2 >> task3
