from datetime import datetime
import random

from airflow import DAG
from airflow.decorators import task


with DAG(
    dag_id="hello_world",
    catchup=False,
    description="My firstly created DAG on this training.",
    start_date=datetime(2023, 1, 23),
    schedule="@hourly",
):
    @task
    def get_name() -> str: 
        print('>> get_name()')
        names = ['jano', 'fero', 'jozo', 'juro', 'anca', 'zuza', 'katka', 'mariena', 'liu']
        return random.choice(names)
    
    
    @task
    def greetings(name: str) -> None:
        print(f'>> greetings("{name.upper()}")')
        return None


    # get_name | greetings
    name = get_name()
    greetings(name)
