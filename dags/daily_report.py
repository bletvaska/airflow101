from pathlib import Path
from tempfile import mkstemp
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import botocore
from pendulum import datetime
import pandas as pd
import pendulum

from helpers import get_minio
from tasks import is_minio_alive


@task(task_display_name="Extract Yesterday Data")
def extract_yesterday_data():
    # setup
    bucket = get_minio().Bucket("datasets")
    path = Path(mkstemp()[1])

    # download
    try:
        bucket.download_file("dataset.csv", path)

        # extract
        df = pd.read_csv(
            path,
            sep=";",
            names=[
                "dt",
                "name",
                "country",
                "temp",
                "hum",
                "press",
                "sunrise",
                "sunset",
                "wind_speed",
                "wind_angle",
            ],
        )

        # prekonvertovanie sekund na cas
        df["dt"] = pd.to_datetime(df["dt"], unit="s")
        df["sunrise"] = pd.to_datetime(df["sunrise"], unit="s")
        df["sunset"] = pd.to_datetime(df["sunset"], unit="s")

        # odstran duplikaty
        df.drop_duplicates(inplace=True)

        # vytvorenie filtra na filtrovanie vcerajsich dat
        f_till_today = df["dt"] < pendulum.today("utc").naive()
        f_since_yesterday = df["dt"] >= pendulum.yesterday("utc").naive()
        filter_yesterday = f_since_yesterday & f_till_today

        # vyfiltrovanie zaznamov
        result = df.loc[filter_yesterday, :]
        print(result)

    except botocore.exceptions.ClientError:
        print("Dataset doesnt't exist in bucket. Possible first time upload.")
        raise AirflowFailException("Dataset is missing.")

    path.unlink(True)


@task(task_display_name="Create Report")
def create_report():
    pass


@dag(
    "daily_report",
    dag_display_name="Daily Report",
    description="Runs once a day to generate report for previous day.",
    schedule="5 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "devops", "dt"],
)
def main():
    is_minio_alive() >> extract_yesterday_data() >> create_report()


if __name__ == "__main__":
    main().test()
else:
    main()
