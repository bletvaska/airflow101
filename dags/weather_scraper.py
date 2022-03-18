from datetime import datetime

from airflow import DAG
from airflow.models import Variable
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


def _filter_data():
    payload = {"coord": {"lon": 19.3034, "lat": 49.2098},
               "weather": [{"id": 803, "main": "Clouds", "description": "broken clouds", "icon": "04d"}],
               "base": "stations",
               "main": {"temp": 8.87, "feels_like": 8.87, "temp_min": 4.35, "temp_max": 9.84, "pressure": 1038,
                        "humidity": 61, "sea_level": 1038, "grnd_level": 975}, "visibility": 10000,
               "wind": {"speed": 1.3, "deg": 285, "gust": 1.84}, "clouds": {"all": 74}, "dt": 1647605063,
               "sys": {"type": 2, "id": 2043555, "country": "SK", "sunrise": 1647579027, "sunset": 1647622285},
               "timezone": 3600, "id": 3060405, "name": "Dolný Kubín", "cod": 200}

    data = payload['main']
    data['dt'] = payload['dt']
    data['wind'] = payload['wind']['speed']
    data['country'] = payload['sys']['country']
    data['city'] = payload['name']

    measurement = Measurement(**data)

    print(measurement)


with DAG('openweathermap_scraper',
         description='Scrapes the weather data from Openweathermap.org',
         schedule_interval='*/15 * * * *',
         start_date=datetime(2022, 3, 18)) as dag:
    task2 = DummyOperator(task_id='process_data')
    task3 = DummyOperator(task_id='store_data')

    scrape_data = SimpleHttpOperator(
        task_id='weather_scrape_data',
        method='GET',
        http_conn_id='openweathermap_api',
        endpoint='/data/2.5/weather',
        data={
            'q': Variable.get('openweathermap_query', default_var='kosice,sk'),
            'units': 'metric',
            'appid': Variable.get('openweathermap_appid')
        },
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
