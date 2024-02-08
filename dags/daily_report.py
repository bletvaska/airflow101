import logging
from pathlib import Path
import tempfile
import jinja2

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import botocore
import pandas as pd
import matplotlib.dates as mdates

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
        return result.to_json(date_format="iso")  # , date_unit='s')  # yesterday data

    except botocore.exceptions.ClientError:
        logger.error("Dataset doesn't exist in bucket.")
        raise AirflowFailException("Dataset doesn't exist in bucket")

    finally:
        if path.exists():
            path.unlink()


@task
def create_report(data: str):
    df = pd.read_json(data, convert_dates=["dt", "sunrise", "sunset"])

    path = Path(__file__).parent / "templates"
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(path), autoescape=False)

    template = env.get_template("weather.tpl.j2")
    # from IPython import embed; embed()

    ts = pendulum.from_timestamp(df.iloc[0]["dt"].timestamp())
    city = df.iloc[0]["city"]
    data = {
        "city": city,
        "date": ts.to_date_string(),
        "max_temp": df["temp"].max(),
        "min_temp": df["temp"].min(),
        "avg_temp": df["temp"].mean(),
        "timestamp": pendulum.now("utc").to_datetime_string(),
    }

    # save to temporary file
    path = Path(tempfile.mkstemp()[1])
    with open(path, "w") as file:
        print(template.render(data), file=file)

    # upload to minio
    minio = get_minio()
    bucket = minio.Bucket("reports")
    bucket.upload_file(path, f"{city}.txt")

    # cleanup
    path.unlink()


@task
def create_plot(data: str):
    df = pd.read_json(data, convert_dates=["dt", "sunrise", "sunset"])

    ax = df.plot(x='dt', y='temp', title='Teplota v meste Košice, 7.2.2024')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H'))
    ax.figure.savefig('kosice.png')


@task
def notify():
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
    [ create_report(data), create_plot(data) ] >> notify()


main()
