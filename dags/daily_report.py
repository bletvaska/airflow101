import logging
from pathlib import Path
import tempfile

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
# from pendulum import datetime
import pendulum
import boto3
import botocore
import pandas as pd

logger = logging.getLogger(__name__)


@task
def is_minio_alive():
    logger.info('MinIO Healthcheck')

    conn = BaseHook.get_connection('minio')
    base_url = f'{conn.schema}://{conn.host}:{conn.port}'

    response = httpx.get(f'{base_url}/minio/health/live')
    if response.status_code != 200:
        logger.error('MinIO is not healthy!')
        raise AirflowFailException('MinIO is not healthy!')


@task
def extract_yesterday_data():
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

        # create filters
        filter_yesterday = (
            df['dt'] >= pendulum.yesterday('utc').to_date_string()
        ) & (
            df['dt'] < pendulum.today('utc').to_date_string()
        )

        # filter yesterday data
        yesterday = df.loc[ filter_yesterday, ['dt', 'temp', 'humidity'] ]
        return yesterday.to_json()

        path.unlink(True)
    except botocore.exceptions.ClientError:
        path.unlink(True)
        message = f"Dataset is missing in bucket {bucket.name}."
        logger.error(message)
        raise AirflowFailException(message)


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
    yesterday_df = is_minio_alive() >> extract_yesterday_data()
    create_report(yesterday_df)


main()
