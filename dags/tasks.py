import logging

from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx

logger = logging.getLogger(__file__)


@task
def is_minio_alive():
    conn = BaseHook.get_connection("minio")
    try:
        response = httpx.get(f"{conn.host}/minio/health/live")
        if response.status_code != 200:
            raise AirflowFailException("MinIO is not alive.")
    except httpx.ConnectError:
        logger.error("Connection error")
        raise AirflowFailException("Connection error")
