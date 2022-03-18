from datetime import datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

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

    scrape_data >> task1 >> task2 >> task3
