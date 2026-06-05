import logging
from http import HTTPStatus

import httpx
from airflow.sdk import task, BaseHook
from airflow.sdk.exceptions import AirflowFailException

from constants import STORAGE_CONN_NAME

logger = logging.getLogger(__name__)


@task(task_display_name="RustFS Healthcheck")
def is_rustfs_alive():
    logger.info("RustFS Healthcheck")

    conn = BaseHook.get_connection(STORAGE_CONN_NAME)

    response = httpx.head(f"{conn.schema}://{conn.host}:{conn.port}/health")

    if response.status_code != HTTPStatus.OK:
        raise AirflowFailException("RustFS is unhealthy.")
