from datetime import datetime
import urllib3

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from minio import Minio
from sqlmodel import SQLModel, Session, create_engine, select

from models import Measurement


with DAG(
    "report_generator",
    description="Generates daily weather reports.",
    schedule="@daily",
    start_date=datetime(2022, 9, 28),
    catchup=False,
):
    @task
    def get_data():
        conn = BaseHook.get_connection("openweathermap_db")
        engine = create_engine(f"{conn.schema}://{conn.host}")
        with Session(engine) as session:
            statement = select(Measurement).where(Measurement.dt > '2022-10-03 00:00:00').where (Measurement.dt < '2022-10-04 00:00:00')
            rows = session.exec(statement).all()


    @task
    def generate_report():
        pass


    @task
    def is_minio_alive():
        """
        Checks if minio and bucket "datasets" are available.

        If the bucket doesn't exist, the function will create it.
        """
        try:
            # create MinIO client
            conn = BaseHook.get_connection('minio_server')
            client = Minio(f'{conn.host}:{conn.port}',
                           conn.login,
                           conn.password,
                           secure=False)

            # check if bucket exists
            if client.bucket_exists('reports') == False:
                client.make_bucket('reports')
        except urllib3.exceptions.MaxRetryError:
            raise AirflowFailException(f'MinIO not available.')


    @task
    def upload_report_to_minio():
        pass


    is_minio_alive() >> get_data() >> generate_report() >> upload_report_to_minio()

