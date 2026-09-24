import logging
from http import HTTPStatus

from airflow.sdk import task, BaseHook
from airflow.sdk.exceptions import AirflowFailException
import httpx

from constants import S3_CONN

logger = logging.getLogger(__name__)


@task(task_display_name="S3 Healthcheck")
def is_rustfs_alive():
    conn = BaseHook.get_connection(S3_CONN)
    url = f"{conn.schema}://{conn.host}:{conn.port}/health"

    response = httpx.head(url)
    if response.status_code != HTTPStatus.OK:
        logger.warning("Something wrong happend.")
        raise AirflowFailException("ta daco nedobre")
