from datetime import datetime
import logging

from airflow.sdk import dag, task

from tasks import is_rustfs_alive


STORAGE_CONN_NAME = "rustfs"

logger = logging.getLogger(__name__)


@task(task_display_name="Extract yesterday data")
def extract_yesterday_data():
    pass


@task(task_display_name="Create report")
def create_report():
    pass


@dag(
    "daily_report",
    dag_display_name="Daily report",
    description="Reports data daily.",
    schedule="5 0 * * *",
    start_date=datetime(2026, 1, 1),
    tags=["mirek", "training", "report"],
    catchup=False,
)
def main():
    is_rustfs_alive() >> extract_yesterday_data() >> create_report()


if __name__ == "__main__":
    main().test()
else:
    main()
