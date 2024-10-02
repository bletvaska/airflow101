from pathlib import Path
from airflow.hooks.base import BaseHook
import boto3
import jinja2

from properties import S3_CONN_NAME


def get_minio():
    # check if bucket datasets exists
    conn = BaseHook.get_connection(S3_CONN_NAME)

    # try:
    client = boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

    return client


def get_jinja():
    path = Path(__file__).parent.parent / 'templates'
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(path),
        autoescape=False
    )
    return env
