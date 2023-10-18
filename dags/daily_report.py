import logging

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
# from pendulum import datetime
import pendulum

logger = logging.getLogger(__name__)


@task
def is_minio_alive():
    logger.info('MinIO Helathcheck')

    conn = BaseHook.get_connection('minio')
    base_url = f'{conn.schema}://{conn.host}:{conn.port}'

    response = httpx.get(f'{base_url}/minio/health/live')
    if response.status_code != 200:
        logger.error('MinIO is not healthy!')
        raise AirflowFailException('MinIO is not healthy!')


@task
def extract_yesterday_data():
    pass


@dag(
    'daily_report',
    start_date=pendulum.datetime(2023, 10, 1),
    schedule='5 0 * * *',
    tags=['weather', 'devops'],
    catchup=False
)
def main():
    is_minio_alive() >> extract_yesterday_data()


main()
