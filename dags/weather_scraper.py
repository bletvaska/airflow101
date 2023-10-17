import json
import logging
from pathlib import Path
import tempfile

from pendulum import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
import httpx
from jsonschema import validate
import boto3
import botocore


logger = logging.getLogger(__name__)
url = "http://api.openweathermap.org/data/2.5/weather"


@task
def is_service_alive():
    logger.info('Openweathermap.org Healthcheck')
    connection = BaseHook.get_connection('openweathermap')
    params = {
        'appid': connection.password
    }
    response = httpx.get(url, params=params)
    if response.status_code == 401:
        logger.error('Invalid API key has been provided.')
        raise AirflowFailException('Invalid API Key.')

    # return None


@task
def scrape_data(query: str) -> dict:
    logger.info("Scraping Data")
    connection = BaseHook.get_connection('openweathermap')
    params = {
        'units': 'metric',
        'q': query,
        'appid': connection.password
    }
    response = httpx.get(url, params=params)
    data = response.json()
    return data


@task
def process_data(data: dict) -> str:
    logger.info("Processing Data")

    # return f'{data["dt"]};' \
    #        f'mesto;krajina;teplota;tlak;vlhkost;sila vetra; smer vetra;vychod slnka;zapad slnka'

    return '{};{};{};{};{};{};{};{};{};{}'.format(
        data['dt'],                 # datetime
        data['name'],               # mesto
        data['sys']['country'],     # krajina
        data['main']['temp'],       # teplota
        data['main']['pressure'],   # tlak
        data['main']['humidity'],   # vlhkost
        data['wind']['speed'],      # vietor - rychlost
        data['wind']['deg'],        # vietor - smer,
        data['sys']['sunrise'],     # vychod slnka
        data['sys']['sunset'],      # zapad slnka
    )


@task
def publish_data(line: str):
    logger.info("Publishing Data")

    # minio client
    minio = boto3.resource('s3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='secret123',
    )

    path = Path(tempfile.mkstemp()[1])
    logger.info(f'Downloading dataset to file {path}')

    # download dataset to temporary file
    try:
        bucket = minio.Bucket('datasets')
        bucket.download_file('dataset.csv', path)
    except botocore.exceptions.ClientError:
        logger.warning("Dataset doesn't exist in bucket. Possible first time upload.")

    # append new measurement as line
    with open(path, mode='a') as dataset:
        print(line, file=dataset)

    # upload dataset
    bucket.upload_file(path, 'dataset.csv')

    # cleanup
    path.unlink(True)


@task
def validate_schema(instance: dict) -> dict:
    with open('/home/ubuntu/airflow/dags/weather.schema.json', mode='r') as fp:
        schema = json.load(fp)

    validate(instance, schema)

    return instance


# [ scrape_data ] -> [validate_schema] -> [ process_data ] -> [ publish_data ]
# scrape_data | validate_schema | process_data | publish_data
@dag(
    'weather_scraper',
    start_date=datetime(2023, 10, 1),
    schedule='*/20 * * * *',
    description='Scrapes weather from openweathermap.org',
    catchup=False,
)
def main(query=Param(type='string', default='kosice,sk', title='Query', description='Name of the city to get wather info.')):
    json_data = is_service_alive() >> scrape_data('kosice,sk')
    validated_data = validate_schema(json_data)
    entry = process_data(validated_data)
    publish_data(entry)


main()
