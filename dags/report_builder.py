import logging

from airflow.sdk import dag, task
from assets import WEATHER_DATA
from pendulum import datetime

from tasks import is_rustfs_alive

logger = logging.getLogger(__name__)


@task(task_display_name="Build Report")
def build_report():
    logger.info("Building Report")


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
    is_rustfs_alive() >> build_report()


main()
