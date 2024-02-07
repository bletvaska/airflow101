import logging

from pendulum import datetime
from airflow.decorators import dag, task

from tasks import is_minio_alive


logger = logging.getLogger(__file__)


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
