import logging
from http import HTTPStatus

import httpx
from airflow.sdk import dag, task, BaseHook
from airflow.sdk.exceptions import AirflowFailException
from pendulum import datetime

from assets import WEATHER_DATA
from constants import S3_CONN


logger = logging.getLogger(__name__)


@task
def ping():
    logger.info("-------------------------> PING")





@dag(
    "report_builder",
    dag_display_name="Report Builder",
    description="Builds report from last weather information.",
    tags=["weather", "devops", "dt", "training"],
    start_date=datetime(2026, 9, 20),
    end_date=datetime(2026, 9, 27),
    catchup=False,
    schedule=[WEATHER_DATA],
)
def main():
    is_rustfs_alive() >> ping()


main()
