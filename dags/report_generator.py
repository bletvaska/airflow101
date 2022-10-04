from datetime import datetime
import urllib3

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from minio import Minio


with DAG(
    "report_generator",
    description="Generates daily weather reports.",
    schedule="@daily",
    start_date=datetime(2022, 9, 28),
    catchup=False,
):
    @task
    def get_data():
        pass


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
            if client.bucket_exists('datasets') == False:
                client.make_bucket('datasets')
        except urllib3.exceptions.MaxRetryError:
            raise AirflowFailException(f'MinIO not available.')


    @task
    def upload_report_to_minio():
        pass


    is_minio_alive() >> get_data() >> generate_report() >> upload_report_to_minio()

