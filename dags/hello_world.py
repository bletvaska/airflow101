from pendulum import datetime
from airflow.decorators import dag, task


@task
def hello() -> str:
    print('>> hello()')
    return 'Hello'


@task
def world(text: str):
    print('>> world()')
    return f'{text} world.'


@dag("hello_world", start_date=datetime(2023, 10, 12), schedule="@daily", catchup=False)
def hello_world():
    # Hello | world
    text = hello()
    world(text)

    # hello >> world


hello_world()
