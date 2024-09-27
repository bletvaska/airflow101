import logging

from airflow.decorators import dag, task
from pendulum import datetime

from tasks import is_minio_alive


logger = logging.getLogger(__name__)


@task()
def extract_yesterday_data():
    logger.info(">> Extracting data from minIO")


@task()
def create_report(data: dict):
    logger.info(">> Creating a report")


@dag(
    "daily_report",
    dag_display_name="Daily Report",
    description="Creates daily weather reports",
    schedule="5 0 * * *",
    start_date=datetime(2024, 1, 1),
    tags=["weather", "devops", "dtit", "report"],
    catchup=False,
)
def main():
    # [ is_minio_alive ] -> [ extract_yesterday_data ] -> [ create_report ]
    extracted_data = is_minio_alive() >> extract_yesterday_data()
    create_report(extracted_data)


if __name__ == "__main__":
    main().test()
else:
    main()
