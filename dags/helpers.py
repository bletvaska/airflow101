from airflow.sdk import BaseHook
import boto3

from constants import S3_CONN


def get_storage():
    conn = BaseHook.get_connection(S3_CONN)
    return boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
