# system modules
from datetime import datetime
from pathlib import Path
import tempfile

# third-party modules
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from botocore.exceptions import ClientError
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import pendulum

# my modules
from tasks import is_minio_alive
from helpers import get_minio_client
from variables import BUCKET_DATASETS, BUCKET_REPORTS, DATASET_WEATHER


@task
def download_dataset() -> str:
    try:
        minio = get_minio_client()

        # get bucket
        bucket = minio.Bucket(BUCKET_DATASETS)

        # create temporary file
        path = tempfile.mkstemp()[1]

        # download weather.csv from S3/MinIO
        bucket.download_file(Key=DATASET_WEATHER, Filename=path)
    except ClientError:
        # print('Given dataset was not found on S3/MinIO.')
        raise AirflowFailException("No weather dataset was found.")

    return path


@task
def create_report(path: str) -> dict:
    # load dataset
    df = pd.read_csv(path, parse_dates=["dt", "sunrise", "sunset"])

    # create filters
    filter1 = df["dt"] >= pendulum.yesterday("utc")
    filter2 = df["dt"] < pendulum.today("utc")
    filter3 = filter1 & filter2

    # create dataset with yesterday entries only
    yesterday = df.loc[filter3]

    # create graph report
    _, ax = plt.subplots()
    yesterday_date = pendulum.yesterday("utc").strftime("%d.%m.%Y")
    ax.set_title(f"Teplota zo dňa {yesterday_date}")
    ax.set_xlabel("čas (hod)")
    ax.set_ylabel("teplota (°C)")
    ax.plot(yesterday["dt"], yesterday["temperature"])
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H"))

    # save report
    _, tmp_path = tempfile.mkstemp(suffix=".png")
    plt.savefig(tmp_path)

    # delete downloaded dataset
    Path(path).unlink(True)

    return {
        "path": tmp_path,
        "date": pendulum.yesterday("utc").strftime("%d.%m.%Y"),
        "city": "kosice",
    }


@task
def publish_report(path: str, date: str, city: str):
    # save figure
    minio = get_minio_client()
    bucket = minio.Bucket(BUCKET_REPORTS)

    # prepare the destination filename
    report_name = f"temperature.at.{date}.in.{city}.png"

    # upload
    bucket.upload_file(Filename=path, Key=report_name)

    # delete downloaded dataset
    Path(path).unlink(True)


with DAG(
    dag_id="create_report",
    catchup=False,
    description="Creates daily weather reports.",
    start_date=datetime(2023, 1, 23),
    schedule="5 0 * * *",
):
    path = is_minio_alive() >> download_dataset()
    report = create_report(path)
    publish_report(report["path"], report["date"], report["city"])
