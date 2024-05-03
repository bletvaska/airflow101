import logging
from pathlib import Path
import tempfile
from pendulum import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import boto3
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowFailException
import pandas as pd
import pendulum

from tasks import healthcheck_minio


logger = logging.getLogger(__name__)


@task
def create_report():
    pass


@task
def extract_yesterday_data():
    # minio client
    conn = BaseHook.get_connection("minio")
    minio = boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

    bucket = minio.Bucket("datasets")

    # create temporary file
    path = Path(tempfile.mkstemp()[1])

    # download dataset
    try:
        bucket.download_file("dataset.csv", path)
    except ClientError:
        logger.warning("Dataset not found. Possibly first run.")
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
    df['dt'] = pd.to_datetime(df['dt'], unit='s')
    
    # create filters
    f_till_today = df['dt'] < pendulum.today('utc').naive()
    f_since_yesterday = df['dt'] >= pendulum.yesterday('utc').naive()
    filter_yesterday = f_till_today & f_since_yesterday
    
    # filter data
    df = df.loc[ filter_yesterday, : ]
    
    logger.info(df)


@dag(
    "daily_report",
    description="daily_report for weather from openweathermap.org",
    schedule="5 0 * * *",
    start_date=datetime(2024, 1, 1),
    tags=["weather", "devops", "t-sys", "tuke"],
    catchup=False,
)
def main():
    healthcheck_minio() >> extract_yesterday_data() >> create_report()


main()
