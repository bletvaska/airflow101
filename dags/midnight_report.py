import logging
from pathlib import Path
import tempfile

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException
import pendulum
from botocore.exceptions import ClientError

from tasks import is_rustfs_alive
from helpers import get_storage
from constants import BUCKET_NAME, DATASET_FILE

logger = logging.getLogger(__name__)


@task(task_display_name="Create Report")
def create_report():
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
