import logging
from pathlib import Path
import tempfile
from pendulum import datetime
from airflow.decorators import dag, task
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
import pandas as pd
import pendulum
import jinja2
import plotly.express as px

from helpers import get_minio
from tasks import healthcheck_minio


logger = logging.getLogger(__name__)


@task
def create_plot(df: pd.DataFrame, ti: TaskInstance):
    # get ready
    exec_date = pendulum.instance(ti.execution_date).start_of("day")
    date = exec_date.add(days=-1).to_date_string()
    df.index = range(0, len(df))
    
    # create figure
    fig = px.line(
        df,
        x="dt",
        y="temp",
        title=f"Teplota v meste {df['city'][0]} zo dňa {date}.",
        line_shape="spline",
        labels={"dt": "čas", "temp": "teplota"},
    )
    
    # save graph as temporary file
    path = Path(tempfile.mkstemp()[1])
    fig.write_image(path, format='png')
    
    # upload to s3/minio
    minio = get_minio()
    bucket = minio.Bucket("reports")
    bucket.upload_file(path, f"{date}.png")
    
    # cleanup
    path.unlink(True)


@task
def create_report(df: pd.DataFrame, ti: TaskInstance):
    # reset index
    df.index = range(0, len(df))

    # prepare model
    exec_date = pendulum.instance(ti.execution_date).start_of("day")

    # from IPython import embed; embed()
    model = {
        "city": df["city"][0],
        "date": exec_date.add(days=-1).to_date_string(),
        "max_temp": df["temp"].max(),
        "min_temp": df["temp"].min(),
        "avg_temp": df["temp"].mean(),
        # "temp_unit": "°C",
        "timestamp": pendulum.now("utc").to_iso8601_string(),
    }

    # prepare jinja2 environment
    tpl_path = Path(__file__).parent / "templates"
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(tpl_path), autoescape=False)

    # get template
    template = env.get_template("weather.tpl.j2")

    # create temporary file
    tmp_path = Path(tempfile.mkstemp()[1])
    with open(tmp_path, "w") as file:
        print(template.render(model), file=file)

    # upload to minio/s3
    minio = get_minio()
    bucket = minio.Bucket("reports")
    bucket.upload_file(tmp_path, f"{exec_date.add(days=-1).to_date_string()}.txt")

    # cleanup
    tmp_path.unlink(True)


@task
def extract_yesterday_data(ti: TaskInstance) -> pd.DataFrame:
    minio = get_minio()
    bucket = minio.Bucket("datasets")

    # create temporary file
    path = Path(tempfile.mkstemp()[1])

    # download dataset
    try:
        bucket.download_file("dataset.csv", path)
    except ClientError:
        logger.warning("Dataset not found.")
        raise AirflowFailException("Dataset not found.")

    # read and clean dataset
    df = pd.read_csv(
        path,
        names=["dt", "city", "temp", "press", "hum", "wind_speed", "wind_deg"],
        sep=",",
    )

    # remove temporary file
    path.unlink(True)

    # cleanup and normalize dataframe
    df.drop_duplicates(inplace=True)
    df["dt"] = pd.to_datetime(df["dt"], unit="s")

    # create filters
    exec_date = pendulum.instance(ti.execution_date).start_of("day")
    f_since_yesterday = df["dt"] >= exec_date.add(days=-1).naive()
    f_till_today = df["dt"] < exec_date.naive()
    filter_yesterday = f_till_today & f_since_yesterday

    # filter data
    df = df.loc[filter_yesterday, :]

    # check if resulting dataframe is not empty
    if len(df) == 0:
        raise AirflowFailException(
            f"No data to create report for date {exec_date.add(days=-1)}"
        )

    # return df.to_json(date_format="iso")
    df.to_csv("/tmp/yesterday.csv")
    return df


@dag(
    "daily_report",
    description="daily_report for weather from openweathermap.org",
    schedule="5 0 * * *",
    start_date=datetime(2024, 1, 1),
    tags=["weather", "devops", "t-sys", "tuke"],
    catchup=False,
)
def main():
    data = healthcheck_minio() >> extract_yesterday_data()
    create_report(data)
    create_plot(data)


main()

# https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html
if __name__ == "__main__":
    main().test()
