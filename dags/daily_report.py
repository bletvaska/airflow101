import logging
from pathlib import Path
import tempfile

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import jinja2
# from pendulum import datetime
import pendulum
import botocore
import pandas as pd

from helper import is_minio_alive, get_minio

logger = logging.getLogger(__name__)


@task
def extract_yesterday_data() -> str:
    # download dataset
    path = Path(tempfile.mkstemp()[1])
    logger.info(f'Downloading dataset to file {path}')

    try:
        # download dataset to temporary file
        bucket = get_minio().Bucket('datasets')
        bucket.download_file('dataset.csv', path)

        # create dataframe
        df = pd.read_csv(
            path,
            delimiter=';',
            names=[
                'dt', 'city', 'country', 'temp', 'pressure', 'humidity', 'wind_speed', 'wind_deg', 'sunrise', 'sunset'
            ]
        )

        # clean and normalize data
        df.drop_duplicates(inplace=True)
        df['dt'] = pd.to_datetime(df['dt'], unit='s')
        df['sunset'] = pd.to_datetime(df['sunset'], unit='s')
        df['sunrise'] = pd.to_datetime(df['sunrise'], unit='s')

        # create filter for yesterdays entries only
        date = pendulum.today('utc')
        filter_yesterday = (
            df['dt'] >= date.add(days=-1).to_date_string()
        ) & (
            df['dt'] < date.to_date_string()
        )

        # filter yesterday data
        yesterday = df.loc[ filter_yesterday, ['dt', 'temp', 'humidity', 'city', 'country'] ]
        return yesterday.to_json()

    except botocore.exceptions.ClientError:
        message = f"Dataset is missing in bucket {bucket.name}."
        logger.error(message)
        raise AirflowFailException(message)

    finally:
        if path.exists():
            path.unlink()


@task
def create_report(data: str):
    # get ready
    df = pd.read_json(data)
    path = Path(__file__).parent / 'templates'

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(path),
        autoescape=False
    )

    # create template
    template = env.get_template('weather.tpl.j2')

    # prepare data
    ts = df['dt'].iloc[0] / 1000
    city = df['city'].iloc[0]
    country = df['country'].iloc[0]
    date = pendulum.from_timestamp(ts).to_date_string()
    # from IPython import embed; embed()
    data = {
        'city': city,
        'country': country,
        'date': date,
        'max_temp': df['temp'].max(),
        'min_temp': df['temp'].min(),
        'avg_temp': df['temp'].mean(),
        'temp_unit': '°C',
        'timestamp': pendulum.now().to_iso8601_string(),
    }

    # save report to file
    path = Path(tempfile.mkstemp()[1])
    with open(path, 'w') as report:
        print(template.render(data), file=report)

    # upload report
    bucket = get_minio().Bucket('reports')
    bucket.upload_file(path, f'{city}_{country}_{date}.txt')

    # cleanup
    path.unlink(True)


@dag(
    'daily_report',
    start_date=pendulum.datetime(2023, 10, 1),
    schedule='5 0 * * *',
    tags=['weather', 'devops'],
    catchup=False
)
def main():
    yesterday = is_minio_alive() >> extract_yesterday_data()
    create_report(yesterday)


main()
