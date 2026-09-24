from airflow.sdk import Asset

from constants import BUCKET_NAME, DATASET_FILE

WEATHER_DATA = Asset(
    name="weather_data", 
    uri=f"s3://{BUCKET_NAME}/{DATASET_FILE}"
)
