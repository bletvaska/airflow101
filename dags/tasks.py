from http import HTTPStatus

import httpx
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException


@task(retries=3)
def healthcheck_minio():
    conn = BaseHook.get_connection("minio")
    url = f"{conn.schema}://{conn.host}:{conn.port}/minio/health/live"
    response = httpx.get(url)
    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException("MinIO service is unhealthy.")
