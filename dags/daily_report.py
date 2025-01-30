from http import HTTPStatus

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from pendulum import datetime
import httpx


@task(task_display_name="MinIO Healthcheck")
def is_minio_alive():
    conn = BaseHook.get_connection("minio")
    url = f"{conn.schema}://{conn.host}:{conn.port}/minio/health/live"

    response = httpx.get(url)

    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException("MinIO service is unhelathy.")


@task(task_display_name="Extract Yesterday Data")
def extract_yesterday_data():
    pass


@task(task_display_name="Create Report")
def create_report():
    pass


@dag(
    "daily_report",
    dag_display_name="Daily Report",
    description="Runs once a day to generate report for previous day.",
    schedule="5 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "devops", "dt"],
)
def main():
    is_minio_alive() >> extract_yesterday_data() >> create_report()


if __name__ == "__main__":
    main().test()
else:
    main()
