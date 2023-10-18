import logging

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
import httpx

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
