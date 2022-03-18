from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor


def _filter_data():
    print('Here we are' * 20)


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
