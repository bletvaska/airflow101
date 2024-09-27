from http import HTTPStatus
import logging

import httpx
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException

from helpers import get_minio
from properties import DATASETS_BUCKET, S3_CONN_NAME


logger = logging.getLogger(__name__)


@task(retries=3)
def is_minio_alive():
    logger.info(">> MinIO Healthcheck")

    conn = BaseHook.get_connection(S3_CONN_NAME)
    url = f"{conn.schema}://{conn.host}:{conn.port}/minio/health/live"
    response = httpx.head(url)

    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException("Minio server not available")

    minio = get_minio()

    # minio.meta.client.head_bucket(Bucket="jano")
    if minio.Bucket(DATASETS_BUCKET).creation_date is None:
        minio.create_bucket(Bucket=DATASETS_BUCKET)

    # try:
    #     minio.create_bucket(Bucket=DATASETS_BUCKET)
    # except minio.meta.client.exceptions.BucketAlreadyExists:
    #     logger.info(f"Bucket {DATASETS_BUCKET} already exists.")
