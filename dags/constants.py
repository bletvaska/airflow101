from pathlib import Path

# airflow home
path = Path(__file__).parent.parent

# path for application data
DATA_PATH = path / "data"
TEMPLATES_PATH = path / 'templates'


# conn names
WEATHER_CONN = "openweathermap"
S3_CONN = "s3"

BUCKET_NAME = 'mirek'

DATASET_FILE = "dataset.csv"

# variables
VAR_FAILURE_NOTIFICATION_URLS = "weather_notification_recipients"
VAR_LOCATIONS = 'weather_locations'
