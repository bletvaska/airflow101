from datetime import datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

with DAG('openweathermap_scraper',
         description='Scrapes the weather data from Openweathermap.org',
         schedule_interval='*/15 * * * *',
         start_date=datetime(2022, 3, 18)) as dag:
    task1 = DummyOperator(task_id='scrape_data')
    task2 = DummyOperator(task_id='process_data')
    task3 = DummyOperator(task_id='store_data')

    scrape_data = SimpleHttpOperator(
        task_id='weather_scrape_data',
        method='GET',
        http_conn_id='openweathermap_api',
        endpoint='/data/2.5/weather',
        data={
            'q': 'kosice',
            'units': 'metric',
            'appid': '08f5d8fd385c443eeff6608c643e0bc5'
        }
    )

    service_availability = HttpSensor(
        task_id='is_weather_api_available',
        http_conn_id='openweathermap_api',
        endpoint='/data/2.5/weather',
        request_params={
            'appid': '08f5d8fd385c443eeff6608c643e0bc5'
        },
        extra_options={'check_response': False},
        poke_interval=10,
        timeout=30
        # retries=3
    )

    service_availability >> scrape_data >> task1 >> task2 >> task3
