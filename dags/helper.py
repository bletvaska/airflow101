import logging

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
import httpx
import boto3

logger = logging.getLogger(__name__)


def get_minio():
    conn = BaseHook.get_connection('minio')
    return boto3.resource('s3',
        endpoint_url=f'{conn.schema}://{conn.host}:{conn.port}',
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )


@task
def is_minio_alive():
    logger.info('MinIO Healthcheck')

    conn = BaseHook.get_connection('minio')
    base_url = f'{conn.schema}://{conn.host}:{conn.port}'

    response = httpx.get(f'{base_url}/minio/health/live')
    if response.status_code != 200:
        logger.error('MinIO is not healthy!')
        raise AirflowFailException('MinIO is not healthy!')
