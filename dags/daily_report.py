import logging
from pathlib import Path
import tempfile

from pendulum import datetime
from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import boto3
import botocore
import pandas as pd
import pendulum

from helper import is_minio_alive

# create logger
logger = logging.getLogger(__name__)  # weather_scraper


@task
def extract_yesterday_data(*args, **kwargs):
    execution_date = kwargs['ti'].execution_date
    
    # get ready
    minio_conn = BaseHook.get_connection("minio")
    minio = boto3.resource(
        "s3",
        endpoint_url=minio_conn.host,
        aws_access_key_id=minio_conn.login,
        aws_secret_access_key=minio_conn.password,
    )

    # download file from s3 to (temporary file)
    bucket = minio.Bucket("datasets")

    # doesn't exist?
    path = Path(tempfile.mkstemp()[1])
    logger.debug(f"Downloading to file {path}.")

    try:
        bucket.download_file("dataset.csv", path)
    except botocore.exceptions.ClientError as ex:
        raise AirflowFailException("Dataset doesn't exist (yet).")

    # load dataset and prepare it
    df = pd.read_csv(path, sep=";")
    df["dt"] = pd.to_datetime(df["dt"], unit="s")

    # create filter for yesterday
    filter_yesterday = (
        (df["dt"] >= pendulum.instance(execution_date).start_of('day').add(days=-1).to_datetime_string()) 
        & (df["dt"] < pendulum.instance(execution_date).start_of('day').to_datetime_string())
    )

    # make query
    yesterday = df.loc[filter_yesterday, ['dt', 'temp', 'hum']]
    
    # cleanup
    path.unlink()
    
    return yesterday.to_json()


@task
def process_data(data: str, *args, **kwargs):
    execution_date = kwargs['ti'].execution_date
    print(execution_date)
    
    df = pd.read_json(data)
    df["dt"] = pd.to_datetime(df["dt"], unit="ms")
    
    print(df)


# DAG definition
@dag(catchup=False, start_date=datetime(2023, 5, 10), schedule="5 0 * * *")
def daily_report():
    data = is_minio_alive() >> extract_yesterday_data()
    process_data(data)


daily_report()


# @task
# def xdebug(*args, **kwargs):
#     print(">>> debug")
#     print(kwargs)
#     print(args)
#     ti: TaskInstance = kwargs['ti']
#     print(TaskInstance.execution_date)

#     dt = ti.execution_date
#     print(dt)
#     print(type(dt))