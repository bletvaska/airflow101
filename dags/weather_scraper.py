import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable, TaskInstance, Connection
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from sqlmodel import SQLModel, Field, create_engine, Session

from models import Measurement

EXPORT_CSV = Path('/airflow/weather.csv')
DB_URI = "sqlite:////airflow/database.db"

logger = logging.getLogger(__name__)

with DAG('openweathermap_scraper',
         description='Scrapes the weather data from Openweathermap.org',
         schedule_interval='*/15 * * * *',
         start_date=datetime(2022, 3, 18),
         catchup=False) as dag:
    service_availability = HttpSensor(
        task_id='is_weather_api_available',
        http_conn_id='openweathermap_api',
        endpoint='/data/2.5/weather',
        request_params={
            'appid': Variable.get('openweathermap_appid'),
            'q': Variable.get('openweathermap_query', default_var='kosice,sk'),
            'units': 'metric',
        },
        poke_interval=10,
        timeout=30,
        method='GET',
    )


    # scrape_data = SimpleHttpOperator(
    #     task_id='scrape_weather_data',
    #     method='GET',
    #     http_conn_id='openweathermap_api',
    #     endpoint='/data/2.5/weather',
    #     data={
    #         'q': Variable.get('openweathermap_query', default_var='kosice,sk'),
    #         'units': 'metric',
    #         'appid': Variable.get('openweathermap_appid')
    #     },
    #     response_filter=lambda response: response.json(),
    #     log_response=True,
    #
    # )

    @task
    def scrape_data():
        # prepare data
        conn = Connection.get_connection_from_secrets('openweathermap_api')
        params = {
            'q': Variable.get('openweathermap_query', default_var='kosice,sk'),
            'units': 'metric',
            'appid': Variable.get('openweathermap_appid')
        }

        with requests.get(f'{conn.host}/data/2.5/weather', params=params) as response:
            # if error, then stop
            if response.status_code != 200:
                raise AirflowFailException('Data were not downloaded properly.')

            return response.json()


    @task
    def preprocess_data(payload: dict):
        # ti: TaskInstance = kwargs['ti']
        # payload = ti.xcom_pull(task_ids=['scrape_weather_data'])[0]

        # prepare data
        try:
            data = payload['main']
            data['dt'] = payload['dt']
            data['wind'] = payload['wind']['speed']
            data['country'] = payload['sys']['country']
            data['city'] = payload['name']

            # create measurement from data
            measurement = Measurement(**data)

            # ti.xcom_push(key='message', value='hello world')
            return measurement.dict()
        except Exception:
            raise AirflowFailException('Data were not correctly formatted.')


    @task
    def export_to_csv_file(data: dict):  # **kwargs):
        # ti = kwargs['ti']
        # data = ti.xcom_pull(task_ids=['preprocess_data'])[0]
        measurement = Measurement(**data)

        with open(EXPORT_CSV, 'a') as file:
            print(measurement.csv(), file=file)


    #
    # send_email = EmailOperator(
    #     task_id='send_email',
    #     to='mirek@cnl.sk',
    #     subject='report is ready',
    #     html_content='the report is almost ready'
    # )

    @task
    def create_table():
        engine = create_engine(DB_URI)
        SQLModel.metadata.create_all(engine)


    @task
    def insert_measurement(data: dict):  # **kwargs):
        # pull data
        # ti = kwargs['ti']
        # data = ti.xcom_pull(task_ids=['preprocess_data'])[0]
        measurement = Measurement(**data)

        # connect to db
        engine = create_engine("sqlite:///database.db")
        # from IPython import embed; embed()

        # insert to db
        with Session(engine) as session:
            logger.info(f'Inserting to db measurement: "{measurement}"')
            session.add(measurement)
            session.commit()


    # task dependencies
    # service_availability >> scrape_data  # >> preprocess_data() >> create_table() >> insert_measurement()
    # scrape_data.downstream_task_ids = ['preprocess_data']
    # scrape_data.set_downstream(task_or_task_list=['preprocess_data'])  # >> preprocess_data()
    # preprocess_data() >> export_to_csv_file()

    payload = service_availability >> scrape_data()

    data = preprocess_data(payload)

    create_table() >> insert_measurement(data)
    export_to_csv_file(data)
