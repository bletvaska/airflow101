from airflow.hooks.base import BaseHook
import boto3


def get_minio():
    # minio client
    conn = BaseHook.get_connection("minio")
    minio = boto3.resource(
        "s3",
        endpoint_url=f"{conn.schema}://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
    return minio
