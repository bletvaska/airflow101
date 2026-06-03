from datetime import datetime

from airflow.sdk import dag

@dag(
    'hello_world',
    dag_display_name='Hello World',
    description='Simple hello world dag example.',
    schedule='*/10 * * * *',
    start_date=datetime(2026, 6, 1),
    tags=['hello', 'world'],
    catchup=False
)
def main():
    pass


main()
