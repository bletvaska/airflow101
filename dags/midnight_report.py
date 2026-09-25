import csv
import logging
from pathlib import Path
import tempfile

import jinja2
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException
import pendulum
from botocore.exceptions import ClientError

from tasks import is_rustfs_alive
from helpers import get_storage
from constants import BUCKET_NAME, DATASET_FILE, TEMPLATES_PATH

logger = logging.getLogger(__name__)

MIDNIGHT_REPORT = "midnight-report.md"


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

        # create Jinja2 environment
        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(TEMPLATES_PATH), autoescape=False
        )

        # load template
        template = env.get_template("midnight-report.tpl.j2")

        # create model
        model = {"datetime": pendulum.now().to_iso8601_string(), "data": []}

        # open dataset
        with open(path) as file:
            reader = csv.DictReader(file, delimiter=";")
            for row in reader:
                model["data"].append(row)

        # render
        report = template.render(model)

        # upload midnight report
        bucket.put_object(
            Key=MIDNIGHT_REPORT,
            Body=report.encode('utf-8'),
            ContextType="text/plain; charset=utf-8"
        )

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


if __name__ == "__main__":
    main().test()
else:
    main()
