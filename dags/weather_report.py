from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from sqlmodel import create_engine

from models import Measurement

with DAG('weather_report',
         description='Creates weather report from scraped data',
         schedule_interval='@daily',
         start_date=datetime(2022, 3, 18),
         catchup=True) as dag:
    @task
    def get_data():
        engine = create_engine("sqlite:///database.db")
        print('get_data')


    @task
    def create_report():
        print('create_report')


    @task
    def send_report():
        pass


    get_data() >> create_report() >> send_report()
