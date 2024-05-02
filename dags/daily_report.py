from http import HTTPStatus

from pendulum import datetime
from airflow.decorators import dag, task
import httpx
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException


@task
def healthcheck_minio():
    conn = BaseHook.get_connection("minio")
    url = f"{conn.schema}://{conn.host}:{conn.port}/minio/health/live"
    response = httpx.get(url)
    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException("MinIO service is unhealthy.")


@task
def create_report():
    pass


@task
def extract_yesterday_data():
    pass


@dag(
    "daily_report",
    description="daily_report for weather from openweathermap.org",
    schedule="5 0 * * *",
    start_date=datetime(2024, 1, 1),
    tags=["weather", "devops", "t-sys", "tuke"],
    catchup=False,
)
def main():
    healthcheck_minio() >> extract_yesterday_data() >> create_report()


main()
