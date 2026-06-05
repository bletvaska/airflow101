from airflow.sdk import BaseHook
import boto3

from constants import STORAGE_CONN_NAME


def get_s3():
    conn = BaseHook.get_connection(STORAGE_CONN_NAME)

    return boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
