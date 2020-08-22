from datetime import datetime
import pandas as pd
from pathlib import Path

from airflow.models import DAG
from airflow.models import Variable
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

dag_name = Path(__file__).stem[4:]

table_variables = Variable.get('cases_world_table',
                               deserialize_json=True)

sql = f"""
CREATE TABLE {table_variables['name']}(
    cases_date DATE,
    cases_day INT,
    cases_month INT,
    cases_year INT,
    cases_cases INT,
    cases_deaths INT,
    cases_country VARCHAR(50),
    cases_geoID CHAR(2),
    cases_country_code CHAR(3),
    cases_population2019 BIGINT,
    cases_continent VARCHAR(10),
    cases_cases14days1000pop DECIMAL(16,4),
    insert_ts TIMESTAMP,
    CONSTRAINT pk_{table_variables['name']} PRIMARY KEY(cases_date, cases_country_code));
"""

default_args = {
    'owner': 'Josu Alonso',
    'start_date': dates.days_ago(1),
    'postgres_conn_id': 'postgres_default'
}


def insert_rows():

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql_insert = f"""INSERT INTO {table_variables['name']}
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cases_df = pd.read_csv(table_variables['url'],
                           parse_dates=['dateRep'],
                           infer_datetime_format=True)
    cases_df.replace({'CNG1925': 'CNG'}, inplace=True)
    insert_ts = datetime.utcnow()

# cases_cases14days1000pop numeric

    for row in cases_df.itertuples(index=False):
        pg_hook.run(sql_insert, parameters=(row[0], row[1], row[2],
                                            row[3], row[4], row[5],
                                            row[6], row[7], row[8],
                                            row[9], row[10], insert_ts))


with DAG(dag_id=dag_name,
         default_args=default_args,
         schedule_interval='@daily') as dag:

    start = DummyOperator(task_id='start')

    drop_table = PostgresOperator(task_id='drop_table',
                                  sql=f"DROP TABLE IF EXISTS {table_variables['name']};")

    create_table = PostgresOperator(task_id='create_table',
                                    sql=sql)

    insert = PythonOperator(task_id='insert',
                            python_callable=insert_rows)


start >> drop_table >> create_table >> insert
