import logging
from pathlib import Path
import tempfile

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import pandas as pd
from pendulum import datetime
import botocore
import pendulum
from pandas.core.frame import DataFrame

from properties import DATASETS_BUCKET, REPORTS_BUCKET
from helpers import get_jinja, get_minio
from tasks import is_minio_alive


logger = logging.getLogger(__name__)


@task()
def extract_yesterday_data(logical_date: pendulum.DateTime) -> DataFrame:
    logger.info(">> Extracting data from MinIO")
    # exec_date = pendulum.instance(ti.execution_date).start_of('day')

    bucket = get_minio().Bucket(DATASETS_BUCKET)

    logging.info(">> Downloading...")
    try:
        _, filename = tempfile.mkstemp()
        tmpfile = Path(filename)

        bucket.download_file("dataset.csv", tmpfile)

        # work with dataset
        df = pd.read_csv(
            tmpfile,
            names=[
                "dt",
                "country",
                "city",
                "temp",
                "humidity",
                "pressure",
                "wind_speed",
                "wind_dir",
            ],
        )
        
        # editing dataset
        df["dt"] = pd.to_datetime(df["dt"], unit="s")
        df.drop_duplicates(inplace=True)
        
        # filters
        tf_yesterday = df["dt"] >= logical_date.start_of('day').subtract(days=1).naive()
        tf_today = df["dt"] < logical_date.start_of('day').naive()

        logging.info(" >> Printing filtered result")
        filtered_data = df.loc[tf_yesterday & tf_today, ["dt", "city", "temp", "humidity"]]
        # print(filtered_data)
        
        return filtered_data

    except botocore.exceptions.ClientError:
        logger.error("Dataset file not found. Posibly no data collected yet.")

        raise AirflowFailException(
            "Dataset file not found. Posibly no data collected yet."
        )

    finally:
        tmpfile.unlink(True)


@task()
def create_report(data: DataFrame):
    logger.info(">> Creating a report")
    
    jinja = get_jinja()
    template = jinja.get_template('weather.tpl.j2')
    
    # from IPython import embed; embed()
    ts = pendulum.from_timestamp(data.iloc[0]['dt'].timestamp())
    city = data.iloc[0]['city']
    
    model = {
        'city': city,
        'date': ts.to_date_string(),
        'max_temp': data['temp'].max(),
        'min_temp': data['temp'].min(),
        'avg_temp': data['temp'].mean(),
        'timestamp': pendulum.now('utc').to_datetime_string(),
        # 'temp_unit': '°C'
    }
    
    # render to temp file
    _, path = tempfile.mkstemp()
    with open(path, 'w') as file:
        print(template.render(model), file=file)
        
    # upload to s3/minio
    bucket = get_minio().Bucket(REPORTS_BUCKET)
    bucket.upload_file(path, f'{city}.txt')
    
    # cleanup
    Path(path).unlink(True)


@dag(
    "daily_report",
    dag_display_name="Daily Report 1d",
    description="Creates daily weather reports",
    schedule="5 0 * * *",
    start_date=datetime(2024, 9, 1),
    tags=["weather", "devops", "dtit", "report"],
    catchup=False,
)
def main(hello: str = 'world'):
    # [ is_minio_alive ] -> [ extract_yesterday_data ] -> [ create_report ]
    extracted_data = is_minio_alive() >> extract_yesterday_data()
    create_report(extracted_data)


if __name__ == "__main__":
    main().test()
else:
    main()
