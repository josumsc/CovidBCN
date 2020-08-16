from datetime import datetime
import pandas as pd
import numpy as np

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook

table_variables = Variable.get('unemployment_gender_table',
                               deserialize_json=True)

default_args = {
    'owner': 'Josu Alonso',
    'start_date': datetime(2020, 7, 1, 0, 0, 0),
    'postgres_conn_id': 'postgres_default'
}

sql = f"""
CREATE TABLE {table_variables['name']} (
  id INT,
  year INT,
  month INT,
  district_code INT,
  district_name VARCHAR(50),
  neighbourhood_code INT,
  neighbourhood_name VARCHAR(100),
  gender VARCHAR(100),
  status VARCHAR(100),
  value BIGINT,
  insert_TS TIMESTAMP,
  CONSTRAINT pk_{table_variables['name']} PRIMARY KEY(id));
"""


def insert_rows():

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql_insert = f"""INSERT INTO {table_variables['name']}
                     VALUES (%s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s)"""

    http_hook = HttpHook(http_conn_id=table_variables['http_conn_id'],
                         method='GET')
    res = http_hook.run(endpoint=table_variables['endpoint'],
                        data={'resource_id': table_variables['resource_id'],
                              'limit': '10000000'})

    http_hook.check_response(response=res)

    unemployment_measures = res.json()['result']['records']

    unemployment_df = pd.DataFrame(unemployment_measures)
    unemployment_df = unemployment_df[['_id', 'Any', 'Mes', 'Codi_Districte', 'Nom_Districte',
                                       'Codi_Barri', 'Nom_Barri', 'Sexe', 'Demanda_ocupacio', 'Nombre']]
    unemployment_df.replace({'NA': np.nan,
                             '-Inf': np.nan,
                             'Inf': np.nan}, inplace=True)
    insert_ts = datetime.utcnow()

    for row in unemployment_df.itertuples(index=False):
        pg_hook.run(sql_insert, parameters=(row[0], row[1], row[2],
                                            row[3], row[4], row[5],
                                            row[6], row[7], row[8],
                                            row[9], insert_ts))


with DAG(dag_id='rep_unemployment_gender',
         default_args=default_args,
         schedule_interval=None) as dag:

    start = DummyOperator(task_id='start')

    drop_table = PostgresOperator(task_id='drop_table',
                                  sql=f"DROP TABLE IF EXISTS {table_variables['name']};")

    create_table = PostgresOperator(task_id='create_table',
                                    sql=sql)

    insert = PythonOperator(task_id='insert',
                            python_callable=insert_rows)


start >> drop_table >> create_table >> insert
