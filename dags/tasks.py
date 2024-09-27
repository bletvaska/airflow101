from http import HTTPStatus
import logging

from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException

from properties import S3_CONN_NAME
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
