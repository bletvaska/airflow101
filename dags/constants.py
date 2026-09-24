from pathlib import Path

# airflow home
path = Path(__file__).parent.parent

# path for application data
DATA_PATH = path / "data"


# conn names
WEATHER_CONN = "openweathermap"
S3_CONN = "s3"
