import logging
from pathlib import Path
import tempfile
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from pendulum import datetime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import botocore
import pandas as pd
import pendulum
from weasyprint import HTML

from helper import get_jinja2, get_minio, is_minio_alive

# create logger
logger = logging.getLogger(__name__)  # weather_scraper


@task
def extract_yesterday_data(*args, **kwargs):
    logging.error('>>> extracting yesterday data')
    execution_date = pendulum.instance(kwargs['ti'].execution_date)
    
    minio = get_minio()

    # download file from s3 to (temporary file)
    bucket = minio.Bucket("datasets")

    # doesn't exist?
    path = Path(tempfile.mkstemp()[1])
    logger.debug(f"Downloading to file {path}.")

    try:
        bucket.download_file("dataset.csv", path)
    except botocore.exceptions.ClientError as ex:
        raise AirflowFailException("Dataset doesn't exist (yet).")

    # load dataset and prepare it
    df = pd.read_csv(path, sep=";")
    df["dt"] = pd.to_datetime(df["dt"], unit="s")

    # create filter for yesterday
    filter_yesterday = (
        (df["dt"] >= execution_date.start_of('day').add(days=-1).to_datetime_string()) 
        & (df["dt"] < execution_date.start_of('day').to_datetime_string())
    )

    # make query
    yesterday = df.loc[filter_yesterday, ['dt', 'temp', 'hum']]
    
    # cleanup
    path.unlink()
    
    return yesterday.to_json()


@task
def process_data(data: str, *args, **kwargs):
    execution_date = pendulum.instance(kwargs['ti'].execution_date)
    
    # get dataframe
    df = pd.read_json(data)
    df["dt"] = pd.to_datetime(df["dt"], unit="ms")
    
    # prepare data
    model = {
        "date": execution_date.add(days=-1).to_date_string(),
        "temp_unit": '°C',
        "max_temp": round(df['temp'].max(), 2),
        "min_temp": round(df['temp'].min(), 2),
        "avg_temp": round(df['temp'].mean(), 2),
        "timestamp": pendulum.now().to_iso8601_string()
    }
    
    # initialize jinja2
    jinja2 = get_jinja2()
    
    # create and render template with data
    template = jinja2.get_template('weather.tpl.j2')
    output = template.render(model)
    
    # create graph
    _, ax = plt.subplots()
    ax.set_title(f"Teplota zo dňa {model['date']}")
    ax.set_xlabel("čas (hod)")
    ax.set_ylabel("teplota (°C)")
    ax.plot(df["dt"].array, df["temp"].array)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H"))

    _, tmp_path = tempfile.mkstemp(suffix=".png")
    plt.savefig('/tmp/graph.png') # tmp_path)
    
    # generate pdf
    html_template = jinja2.get_template('weather.tpl.html.j2')
    output_html = html_template.render(model)
    HTML(string=output_html).write_pdf('report.pdf')
    
    minio = get_minio()
    bucket = minio.Bucket('reports')
    
    path = Path(tempfile.mkstemp()[1])
    with open(path, mode='w') as file:
        file.write(output)
        # print(output, file=file)
        
    # 2023-05-18_kosice.txt
    bucket.upload_file(path, f'{model["date"]}_kosice.txt')
    bucket.upload_file('report.pdf', f'{model["date"]}_kosice.pdf')
    
    # cleanup
    path.unlink()


# DAG definition
@dag(catchup=True, start_date=datetime(2023, 5, 1), schedule="5 0 * * *")
def daily_report():
    data = is_minio_alive() >> extract_yesterday_data()
    process_data(data)


daily_report()
