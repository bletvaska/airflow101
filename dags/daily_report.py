import logging

import httpx
from pendulum import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__file__)


@task
def is_minio_alive():
    conn = BaseHook.get_connection("minio")
    try:
        response = httpx.get(f"{conn.host}/minio/health/live")
        if response.status_code != 200:
            raise AirflowFailException("MinIO is not alive.")
    except httpx.ConnectError:
        logger.error("Connection error")
        raise AirflowFailException("Connection error")


@task
def extract_yesterday_data():
    pass


@task
def create_report():
    pass


@dag(
    "daily_report",
    description="Creates daily weather report.",
    schedule="5 0 * * *",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    tags=["devops", "telekom", "weather", "daily"],
    catchup=False,
)
def main():
    is_minio_alive() >> extract_yesterday_data() >> create_report()


main()
