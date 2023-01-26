# system modules
from datetime import datetime

# third-party modules
from airflow import DAG
from airflow.decorators import task

# my modules
from tasks import is_minio_alive

@task
def download_dataset():
    return 'path'

@task
def create_report():
    pass

@task
def publish_report():
    pass



with DAG(
    dag_id="create_report",
    catchup=False,
    description="Creates daily weather reports.",
    start_date=datetime(2023, 1, 23),
    schedule="5 0 * * *",
):
    is_minio_alive() >> download_dataset() >> create_report() >> publish_report()
