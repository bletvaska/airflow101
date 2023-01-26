from airflow.decorators import task
from airflow.exceptions import AirflowFailException
import httpx


@task
def is_minio_alive() -> None:
    # make request
    url = "http://localhost:9000/minio/health/live"
    response = httpx.head(url)

    # not 200, then stop DAG
    if response.status_code != 200:
        raise AirflowFailException("MinIO is not alive.")
