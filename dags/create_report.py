# system modules
from datetime import datetime
from pathlib import Path
import tempfile

# third-party modules
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
import boto3
from botocore.exceptions import ClientError

# my modules
from tasks import is_minio_alive

BUCKET_DATASETS = 'datasets'
DATASET_WEATHER = 'weather.csv'

@task
def download_dataset():
    # create minio/s3 client
    minio = boto3.resource(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="jahodka123",
        # config=boto3.session.Config(signature_version='s3v4'),
        # aws_session_token=None,
        # verify=False
    )
    
    # get bucket
    bucket = minio.Bucket(BUCKET_DATASETS)
    
    # create temporary file
    path = Path(tempfile.mkstemp()[1])
    
    # download weather.csv from S3/MinIO
    try:
        bucket.download_file(Key=DATASET_WEATHER, Filename=path)
    except ClientError:
        # print('Given dataset was not found on S3/MinIO.')
        raise AirflowFailException("No weather dataset was found.")
    
    return str(path)

@task
def create_report():
    pass

@task
def publish_report():
    pass



with DAG(
    dag_id="create_report",
    catchup=False,
    description="Creates daily weather reports.",
    start_date=datetime(2023, 1, 23),
    schedule="5 0 * * *",
):
    is_minio_alive() >> download_dataset() >> create_report() >> publish_report()
