import logging
from pathlib import Path
import tempfile

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import boto3
import botocore
import pandas as pd

from tasks import is_minio_alive


logger = logging.getLogger(__file__)
DATASET = "weather.csv"


@task
def extract_yesterday_data() -> str:
    # download dataset as dataframe
    conn = BaseHook.get_connection("minio")
    minio = boto3.resource(
        "s3",
        endpoint_url=conn.host,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

    tmpfile = tempfile.mkstemp()[1]
    path = Path(tmpfile)
    logger.info(f"Downloading dataset to file {path}")

    bucket = minio.Bucket("datasets")
    try:
        bucket.download_file(DATASET, path)
    except botocore.exceptions.ClientError:
        logger.warning("Dataset doesn't exist in bucket. Possible first time upload.")

    df = pd.read_csv(
        path,
        names=[
            "dt",
            "city",
            "country",
            "temp",
            "hum",
            "press",
            "sunrise",
            "sunset",
            "wind_angle",
            "wind_speed",
        ],
    )

    # cleanup dataframe
    df["dt"] = pd.to_datetime(df["dt"], unit="s")
    df["sunrise"] = pd.to_datetime(df["sunrise"], unit="s")
    df["sunset"] = pd.to_datetime(df["sunset"], unit="s")
    df.drop_duplicates(inplace=True)

    # filter yesterday data
    filter_from_yesterday = df["dt"] >= pendulum.yesterday("utc").to_date_string()
    filter_till_today = df["dt"] < pendulum.today("utc").to_date_string()
    filter_yesterday = filter_from_yesterday & filter_till_today

    result = df.loc[filter_yesterday, :]
    return result.to_json()  # yesterday data


@task
def create_report(data: str):
    print(data)
    pass


@dag(
    "daily_report",
    description="Creates daily weather report.",
    schedule="5 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["devops", "telekom", "weather", "daily"],
    catchup=False,
)
def main():
    data = is_minio_alive() >> extract_yesterday_data()
    create_report(data)


main()
