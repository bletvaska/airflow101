import logging

from airflow.sdk import dag, task
import pendulum

from tasks import is_rustfs_alive

logger = logging.getLogger(__name__)


@task(task_display_name="Create Report")
def create_report():
    pass


@dag(
    "midnight_report",
    dag_display_name="Midnight Report",
    description="Creates daily/midnight report.",
    schedule="15 0 * * *",
    tags=["dt", "devops", "airflow", "training"],
    start_date=pendulum.datetime(2026, 9, 24),
    end_date=pendulum.datetime(2026, 9, 30),
    catchup=False,
)
def main():
    is_rustfs_alive() >> create_report()


main()
