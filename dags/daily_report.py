from pathlib import Path
from tempfile import mkstemp
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
import botocore
from pendulum import datetime
import pandas as pd
import pendulum
from apprise import Apprise

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
        path.unlink(True)

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
        return result

    except botocore.exceptions.ClientError:
        print("Dataset doesnt't exist in bucket. Possible first time upload.")
        raise AirflowFailException("Dataset is missing.")


@task(task_display_name="Notify")
def notify(message: str):
    apprise = Apprise()
    token = Variable.get('PUSHBULLET_TOKEN')
    apprise.add(f'pbul://{token}')
    apprise.notify(title='Denný report', body=message)


@task(task_display_name="Create Report")
def create_report(df: pd.DataFrame) -> str:
    max = round(df["temp"].max(), 1)
    min = round(df["temp"].min(), 1)
    mean = round(df["temp"].mean(), 1)

    entry = df.iloc[0]
    date = entry['dt'].date()
    name = entry['name']
    country = entry['country']

    return f'Dňa {date} sa teplota v meste {name} ({country}) pohybovala v rozmedzí od {min}°C do {max}°C (priemerná teplota bola {mean}°C).'


@task(task_display_name="Create PDF Report")
def create_pdf_report(message: str):
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
    data = is_minio_alive() >> extract_yesterday_data()
    report = create_report(data)
    notify(report)
    create_pdf_report(report)


if __name__ == "__main__":
    main().test()
else:
    main()
