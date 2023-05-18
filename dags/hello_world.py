from pendulum import datetime
from airflow.decorators import task, dag
from airflow.models.param import Param


@task
def get_name(name: str) -> str:
    print(">> get_name()")
    if name is None:
        return "jano"
    else:
        return name


@task
def greetings(name: str | Param):
    print(">> greetings()")
    print(f"Hello {name}!")


@dag(
    description="Simple Hello world DAG.",
    catchup=False,
    start_date=datetime(2023, 5, 1),
    schedule=None,
    tags=["training", "t-systems"],
)
def hello_world(
    new_name=Param(
        default="janko", type="string", title="Name", description="Whom to greet."
    ),
    query=Param(
        default='kosice', type='string', title='City', description='name of the city'
    )
):
    # get_name | greetings
    name = get_name(new_name)
    greetings(name)


hello_world()
