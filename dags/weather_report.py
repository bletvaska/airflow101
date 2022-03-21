from datetime import datetime

import sqlmodel
from airflow import DAG
from airflow.decorators import task
from airflow.models import Connection
from airflow.operators.email import EmailOperator
from jinja2 import Environment, FileSystemLoader
from sqlmodel import create_engine, Session, select

# from weasyprint import HTML

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

            # aggregated values
            statement = select(
                sqlmodel.func.min(Measurement.temp),
                sqlmodel.func.max(Measurement.temp),
                sqlmodel.func.min(Measurement.humidity),
                sqlmodel.func.max(Measurement.humidity),
            )
            values = session.exec(statement).one()

            # get all data
            statement = select(Measurement)
            measurements = []
            # from IPython import embed;
            # embed()


            for measurement in session.exec(statement).all():
                entry = measurement.dict()
                measurements.append(entry)

            # from IPython import embed;
            # embed()
            return {
                'tempMax': values[1],
                'tempMin': values[0],
                'humidityMax': values[3],
                'humidityMin': values[2],
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
        output = template.render(**data)

        file = open('/tmp/report.html', 'w')
        print(output, file=file)
        file.close()

        # HTML(string=output).write_pdf('/airflow/reports/weather.pdf')

        # from IPython import embed; embed()


    @task
    def create_csv_report(data: dict):
        with open('/airflow/reports/report.csv', 'w') as file:
            for entry in data['measurements']:
                measurement = Measurement(**entry)
                print(measurement.csv(), file=file)


    send_email = EmailOperator(
        task_id='send_email',
        to='mirek@cnl.sk',
        subject='report is ready',
        html_content='the report is almost ready',
        files=[
            '/airflow/reports/weather.csv'
        ]
    )

    # DAG
    data = get_data()
    [create_txt_report(data),
     create_md_report(data),
     create_html_report(data),
     create_csv_report(data)] >> send_email
