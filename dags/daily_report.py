from datetime import datetime
import logging
from pathlib import Path
from tempfile import mkstemp

from airflow.sdk import dag, task, get_current_context
import pandas as pd
import pendulum
from pyspark.sql import SparkSession

from tasks import is_rustfs_alive
from helpers import get_s3
from constants import DATASET_BUCKET


logger = logging.getLogger(__name__)


@task(task_display_name="Extract yesterday data")
def extract_yesterday_data() -> str:
    # get ready
    storage = get_s3()
    bucket = storage.bucket(DATASET_BUCKET)
    temp_file = Path(mkstemp(prefix="dataset-")[1])

    try:
        # download dataset
        bucket.download_file(
            "dataset.csv",
            temp_file
        )

        # create dataframe
        df = pd.read_csv(temp_file, parse_dates=["dt", "sunrise", "sunset"])

        context = get_current_context()

        # čas spustenia celého DAG runu 
        logical_date = context["logical_date"]
        yesterday = logical_date.subtract(days=1).date()

        # filter data
        result = df.loc[ df['dt'].dt.date == yesterday ]

        # return as CSV
        return result.to_csv()

    except Exception as ex:
        logger.error('sa to zrubalo')
        logger.exception(ex)


@task(task_display_name="Create report")
def create_report(dataset: str):
    # get max/min/avg temperature/humidity/pressure from yesterday data
    print(dataset)


@task(task_display_name="Extract Yesterday Data with PySpark")
def pyspark_extract_yesterday_data() -> str:
    # get ready
    storage = get_s3()
    bucket = storage.Bucket(DATASET_BUCKET)
    temp_file = Path(mkstemp(prefix="dataset-")[1])

    try:
        # download dataset
        bucket.download_file(
            "kosice-sk.csv",
            temp_file
        )

        spark = SparkSession.builder \
                .appName('YesterdayData') \
                .getOrCreate()

        # create dataframe
        df = spark.read.csv(str(temp_file), header=True, inferSchema=True)

        # filter data
        result = df.filter(df['dt'].between('2026-06-07', '2026-06-08'))

        return result

    except Exception as ex:
        logger.exception(ex)



@dag(
    "daily_report",
    dag_display_name="Daily report",
    description="Reports data daily.",
    schedule="5 0 * * *",
    start_date=datetime(2026, 1, 1),
    tags=["mirek", "training", "report"],
    catchup=False,
)
def main():
    dataset = is_rustfs_alive() >> extract_yesterday_data()
    create_report(dataset)

    pyspark_extract_yesterday_data()

if __name__ == "__main__":
    main().test(logical_date=pendulum.parse("2026-06-03 00:00:00+00:00"))
else:
    main()
