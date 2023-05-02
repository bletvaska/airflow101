from datetime import datetime

from airflow import DAG
from airflow.decorators import task


with DAG(
    "hello_world",
    description="Simple Hello world DAG.",
    schedule="@daily",
    start_date=datetime(2023, 4, 29),
    catchup=False,
    tags=["training", "t-systems"],
):

    @task
    def get_name() -> str:
        print(">> get_name()")
        return 'jano'

    @task
    def greetings(name: str):
        print(">> greetings()")
        print(f'Hello {name}!')

    # get_name | greetings
    name = get_name()
    greetings(name)
