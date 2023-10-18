import logging
from pathlib import Path
import tempfile

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
# from pendulum import datetime
import pendulum
import boto3
import botocore
import pandas as pd

from helper import is_minio_alive

logger = logging.getLogger(__name__)


@task
def extract_yesterday_data() -> str:
    # download dataset
    conn = BaseHook.get_connection('minio')
    minio = boto3.resource('s3',
        endpoint_url=f'{conn.schema}://{conn.host}:{conn.port}',
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

    path = Path(tempfile.mkstemp()[1])
    logger.info(f'Downloading dataset to file {path}')

    try:
        # download dataset to temporary file
        bucket = minio.Bucket('datasets')
        bucket.download_file('dataset.csv', path)

        # create dataframe
        df = pd.read_csv(
            path,
            delimiter=';',
            names=[
                'dt', 'city', 'country', 'temp', 'pressure', 'humidity', 'wind_speed', 'wind_deg', 'sunrise', 'sunset'
            ]
        )

        # clean and normalize data
        df.drop_duplicates(inplace=True)
        df['dt'] = pd.to_datetime(df['dt'], unit='s')
        df['sunset'] = pd.to_datetime(df['sunset'], unit='s')
        df['sunrise'] = pd.to_datetime(df['sunrise'], unit='s')

        # create filter for yesterdays entries only
        date = pendulum.today('utc')
        filter_yesterday = (
            df['dt'] >= date.add(days=-1).to_date_string()
        ) & (
            df['dt'] < date.to_date_string()
        )

        # filter yesterday data
        yesterday = df.loc[ filter_yesterday, ['dt', 'temp', 'humidity'] ]
        return yesterday.to_json()

    except botocore.exceptions.ClientError:
        message = f"Dataset is missing in bucket {bucket.name}."
        logger.error(message)
        raise AirflowFailException(message)

    finally:
        if path.exists():
            path.unlink()


@task
def create_report(data: str):
    df = pd.read_json(data)
    print(df)


@dag(
    'daily_report',
    start_date=pendulum.datetime(2023, 10, 1),
    schedule='5 0 * * *',
    tags=['weather', 'devops'],
    catchup=False
)
def main():
    yesterday = is_minio_alive() >> extract_yesterday_data()
    create_report(yesterday)


main()
