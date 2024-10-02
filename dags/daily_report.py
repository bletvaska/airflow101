import json
import logging
from pathlib import Path
import tempfile

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
import pandas as pd
from pendulum import datetime
import botocore
import pendulum
from pandas.core.frame import DataFrame

from properties import DATASETS_BUCKET
from helpers import get_minio
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
        filtered_data = df.loc[tf_yesterday & tf_today, ["dt", "temp", "humidity"]]
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
def create_report(data):
    logger.info(">> Creating a report")
    
    print(data)


@dag(
    "daily_report",
    dag_display_name="Daily Report 1d",
    description="Creates daily weather reports",
    schedule="5 0 * * *",
    start_date=datetime(2024, 1, 1),
    tags=["weather", "devops", "dtit", "report"],
    catchup=False,
)
def main():
    # [ is_minio_alive ] -> [ extract_yesterday_data ] -> [ create_report ]
    extracted_data = is_minio_alive() >> extract_yesterday_data()
    create_report(extracted_data)


if __name__ == "__main__":
    main().test()
else:
    main()
