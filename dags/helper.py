from functools import lru_cache
import logging
from pathlib import Path
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
import boto3
from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)


@task
def is_minio_alive():
    logger.info("is minio alive")

    base_url = BaseHook.get_connection("minio").host
    response = httpx.head(f"{base_url}/minio/health/live")

    if response.status_code != 200:
        logger.critical("Minio is not Alive")
        raise AirflowFailException("MinIo is not Alive.")


@lru_cache
def get_minio():
    # get ready
    minio_conn = BaseHook.get_connection("minio")
    minio = boto3.resource(
        "s3",
        endpoint_url=minio_conn.host,
        aws_access_key_id=minio_conn.login,
        aws_secret_access_key=minio_conn.password,
    )
    
    return minio
    
    
def get_jinja2():
    path = Path(__file__)
    env = Environment(
        loader=FileSystemLoader(path.parent / 'templates'),
        autoescape=False
    )
    
    return env