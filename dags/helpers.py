import boto3
from airflow.hooks.base import BaseHook


def get_minio():
    conn = BaseHook.get_connection("minio")
    return boto3.resource(
        "s3",
        endpoint_url=conn.host,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
