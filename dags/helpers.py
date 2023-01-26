import boto3


def get_minio_client():
    # create minio/s3 client
    minio = boto3.resource(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="jahodka123",
        # config=boto3.session.Config(signature_version='s3v4'),
        # aws_session_token=None,
        # verify=False
    )

    return minio
