import logging
from pendulum import datetime
from airflow.decorators import dag, task

from tasks import healthcheck_minio


logger = logging.getLogger(__name__)


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
