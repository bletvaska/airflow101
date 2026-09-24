import logging
from pathlib import Path
import tempfile

from airflow.sdk import dag, task, BaseHook
from airflow.exceptions import AirflowFailException
from assets import WEATHER_DATA
from pendulum import datetime
import boto3
from botocore.exceptions import ClientError

from tasks import is_rustfs_alive
from constants import BUCKET_NAME, DATASET_FILE, S3_CONN
from helpers import get_storage

logger = logging.getLogger(__name__)


@task(task_display_name="Build Report")
def build_report():
    logger.info("Building Report")

    # stiahni dataset
    # (ak sa nepodarilo, tak skonci s chybou)
    storage = get_storage()


    bucket = storage.Bucket(BUCKET_NAME)

    try:
        path = Path(tempfile.mkstemp()[1])

        try:
            bucket.download_file(DATASET_FILE, path)
        except ClientError as ex:
            logger.warning("Dataset file not found. Nothing to do.")
            raise AirflowFailException("Dataset file not found. Nothing to do.")

        # magic


    finally:
        # clean up
        # (stiahnuty subor sa zmaze)
        path.unlink(True)


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
