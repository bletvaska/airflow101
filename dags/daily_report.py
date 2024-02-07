import logging
from pathlib import Path
import tempfile

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import botocore
import pandas as pd

from helpers import get_minio
from tasks import is_minio_alive


logger = logging.getLogger(__file__)
DATASET = "weather.csv"


@task
def extract_yesterday_data() -> str:
    # download dataset as dataframe
    minio = get_minio()

    tmpfile = tempfile.mkstemp()[1]
    path = Path(tmpfile)
    logger.info(f"Downloading dataset to file {path}")

    bucket = minio.Bucket("datasets")
    try:
        bucket.download_file(DATASET, path)

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

    except botocore.exceptions.ClientError:
        logger.error("Dataset doesn't exist in bucket.")
        raise AirflowFailException("Dataset doesn't exist in bucket")

    finally:
        if path.exists():
            path.unlink()


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
