from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Connection
from sqlmodel import create_engine, Session

from models import Measurement

with DAG('weather_report',
         description='Creates weather report from scraped data',
         schedule_interval='@daily',
         start_date=datetime(2022, 3, 18),
         catchup=True) as dag:
    @task
    def get_data():
        # connect to db
        conn = Connection.get_connection_from_secrets('weather_db_uri')
        engine = create_engine(conn.host)

        # query db
        with Session(engine) as session:
            from IPython import embed; embed()


    @task
    def create_report():
        print('create_report')


    @task
    def send_report():
        pass


    get_data() >> create_report() >> send_report()
