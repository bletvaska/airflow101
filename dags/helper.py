import logging
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx

logger = logging.getLogger(__name__)


@task
def is_minio_alive():
    logger.info("is minio alive")

    base_url = BaseHook.get_connection("minio").host
    response = httpx.head(f"{base_url}/minio/health/live")

    if response.status_code != 200:
        logger.critical("Minio is not Alive")
        raise AirflowFailException("MinIo is not Alive.")
