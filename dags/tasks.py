from http import HTTPStatus

from airflow.hooks.base import BaseHook
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
import httpx


@task(task_display_name="MinIO Healthcheck")
def is_minio_alive():
    conn = BaseHook.get_connection("minio")
    url = f"{conn.schema}://{conn.host}:{conn.port}/minio/health/live"

    response = httpx.get(url)

    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException("MinIO service is unhelathy.")
