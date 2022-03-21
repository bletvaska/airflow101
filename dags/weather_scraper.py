from datetime import datetime
from pathlib import Path
from typing import Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from sqlmodel import SQLModel, Field, create_engine, Session

EXPORT_CSV = Path('/airflow/weather.csv')
DB_URI = "sqlite:////airflow/database.db"


class Measurement(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dt: int
    temp: float
    pressure: int
    humidity: int
    wind: float
    country: str
    city: str

    def csv(self, separator=','):
        data = [str(self.dt), str(self.temp), str(self.pressure), str(self.humidity), str(self.wind), self.country,
                self.city]
        return separator.join(data)




def _export_to_csv_file(ti: TaskInstance):
    payload = ti.xcom_pull(task_ids=['preprocess_data'])[0]
    measurement = Measurement(**payload)

    with open(EXPORT_CSV, 'a') as file:
        print(measurement.csv(), file=file)


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

    return measurement.dict()


def _helper_operator(ti: TaskInstance):
    payload = ti.xcom_pull(task_ids=['preprocess_data'])[0]
    measurement = Measurement(**payload)
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
            'appid': Variable.get('openweathermap_appid'),
            'q': Variable.get('openweathermap_query', default_var='kosice,sk'),
            'units': 'metric',
        },
        poke_interval=10,
        timeout=30,
        method='GET',
    )

    data_preprocessor = PythonOperator(
        task_id='preprocess_data',
        python_callable=_filter_data
    )

    # PythonOperator(
    #     task_id='helper_operator',
    #     python_callable=_helper_operator
    # )

    to_csv = PythonOperator(
        task_id='export_to_csv',
        python_callable=_export_to_csv_file
    )


    send_email = EmailOperator(
        task_id='send_email',
        to='mirek@cnl.sk',
        subject='report is ready',
        html_content='the report is almost ready'
    )

    @task
    def create_table():
        engine = create_engine(DB_URI)
        SQLModel.metadata.create_all(engine)

    @task
    def insert_measurement(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids=['preprocess_data'])[0]
        measurement = Measurement(**data)
        from IPython import embed; embed()

        # engine = create_engine("sqlite:///database.db")
        # with Session(engine) as session:
        #     session.add()



    service_availability >> scrape_data >> data_preprocessor >> [to_csv, create_table()]
    create_table() >> insert_measurement()

