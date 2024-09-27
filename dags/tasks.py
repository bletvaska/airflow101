from http import HTTPStatus
import logging

from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import boto3

from properties import DATASETS_BUCKET, S3_CONN_NAME
import httpx


logger = logging.getLogger(__name__)


@task(retries=3)
def is_minio_alive():
    logger.info(">> MinIO Healthcheck")

    conn = BaseHook.get_connection(S3_CONN_NAME)
    url = f"{conn.schema}://{conn.host}:{conn.port}/minio/health/live"
    response = httpx.head(url)

    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException("Minio server not available")

    # check if bucket datasets exists
    conn = BaseHook.get_connection(S3_CONN_NAME)

    # try:
    minio = boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

    # minio.meta.client.head_bucket(Bucket="jano")
    if minio.Bucket(DATASETS_BUCKET).creation_date is None:
        minio.create_bucket(Bucket=DATASETS_BUCKET)

    # try:
    #     minio.create_bucket(Bucket=DATASETS_BUCKET)
    # except minio.meta.client.exceptions.BucketAlreadyExists:
    #     logger.info(f"Bucket {DATASETS_BUCKET} already exists.")
