import logging
from pathlib import Path
import tempfile
from pendulum import datetime
from airflow.decorators import dag, task
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
import pandas as pd
import pendulum

from helpers import get_minio
from tasks import healthcheck_minio


logger = logging.getLogger(__name__)


@task
def create_report(data: str, ti: TaskInstance):
    df = pd.read_json(data)
    logger.info(df)


@task
def extract_yesterday_data(ti: TaskInstance) -> str:
    minio = get_minio()
    bucket = minio.Bucket("datasets")

    # create temporary file
    path = Path(tempfile.mkstemp()[1])

    # download dataset
    try:
        bucket.download_file("dataset.csv", path)
    except ClientError:
        logger.warning("Dataset not found.")
        raise AirflowFailException("Dataset not found.")

    # read and clean dataset
    df = pd.read_csv(
        path,
        names=["dt", "city", "temp", "press", "hum", "wind_speed", "wind_deg"],
        sep=",",
    )

    # remove temporary file
    path.unlink(True)

    # cleanup and normalize dataframe
    df.drop_duplicates(inplace=True)
    df["dt"] = pd.to_datetime(df["dt"], unit="s")

    # create filters
    exec_date = pendulum.instance(ti.execution_date).start_of('day')
    f_since_yesterday = df["dt"] >= exec_date.add(days=-1).naive()
    f_till_today = df["dt"] < exec_date.naive()
    filter_yesterday = f_till_today & f_since_yesterday
    #from IPython import embed; embed()

    # filter data
    df = df.loc[filter_yesterday, :]

    return df.to_json(date_format="iso")


@task
def debug(ti: TaskInstance):
    logger.info(ti.execution_date)

    # from IPython import embed; embed()


@dag(
    "daily_report",
    description="daily_report for weather from openweathermap.org",
    schedule="5 0 * * *",
    start_date=datetime(2024, 1, 1),
    tags=["weather", "devops", "t-sys", "tuke"],
    catchup=False,
)
def main():
    data = debug() >> healthcheck_minio() >> extract_yesterday_data()
    create_report(data)


main()
