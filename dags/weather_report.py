from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Connection
from jinja2 import Environment, FileSystemLoader
from sqlmodel import create_engine, Session, select

from models import Measurement

with DAG('weather_report',
         description='Creates weather report from scraped data',
         schedule_interval='@daily',
         start_date=datetime(2022, 3, 18),
         catchup=True) as dag:
    @task
    def get_data():
        # connect to db
        conn = Connection.get_connection_from_secrets('weather_db_uri')
        engine = create_engine(conn.host)

        # query db
        with Session(engine) as session:
            # get temp
            statement = select(Measurement.temp)
            temp_max = max(session.exec(statement).all())
            temp_min = min(session.exec(statement).all())

            # get humidity
            statement = select(Measurement.humidity)
            humidity_max = max(session.exec(statement).all())
            humidity_min = min(session.exec(statement).all())

            # get all data
            statement = select(Measurement)
            measurements = []

            for entry in session.exec(statement).all():
                measurements.append(entry.dict())

            # from IPython import embed;
            # embed()

            return {
                'tempMax': temp_max,
                'tempMin': temp_min,
                'humidityMax': humidity_max,
                'humidityMin': humidity_min,
                'measurements': measurements
            }


    @task
    def create_txt_report(data: dict):
        jenv = Environment(
            loader=FileSystemLoader('/airflow/templates/'),
        )
        template = jenv.get_template('weather.report.tpl.txt')
        print(template.render(**data))

        # from IPython import embed; embed()


    @task
    def create_md_report(data: dict):
        jenv = Environment(
            loader=FileSystemLoader('/airflow/templates/'),
        )
        template = jenv.get_template('weather.report.tpl.md')
        print(template.render(**data))

        # from IPython import embed; embed()


    @task
    def create_html_report(data: dict):
        jenv = Environment(
            loader=FileSystemLoader('/airflow/templates/'),
        )
        template = jenv.get_template('weather.report.tpl.html')
        print(template.render(**data))

        # from IPython import embed; embed()


    @task
    def create_csv_report(data: dict):
        with open('/airflow/reports/report.csv', 'w') as file:
            for entry in data['measurements']:
                measurement = Measurement(**entry)
                print(measurement.csv(), file=file)


    @task
    def send_report():
        pass


    data = get_data()
    [create_txt_report(data),
     create_md_report(data),
     create_html_report(data),
     create_csv_report(data)] >> send_report()
