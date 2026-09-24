from airflow.sdk import dag, task
from pendulum import datetime

from assets import WEATHER_DATA


@dag(
    'report_builder',
    dag_display_name='Report Builder',
    description='Builds report from last weather information.',
    tags=["weather", "devops", "dt", "training"],
    start_date=datetime(2026, 9, 20),
    end_date=datetime(2026, 9, 27),
    catchup=False,
    schedule=[ WEATHER_DATA ],
)
def main():
    pass

main()
