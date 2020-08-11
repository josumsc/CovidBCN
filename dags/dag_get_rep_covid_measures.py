from datetime import datetime
import pandas as pd
import numpy as np

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook


table_variables = Variable.get('covid_measures_table',
                               deserialize_json=True)

default_args = {
    'owner': 'Josu Alonso',
    'start_date': datetime(2020, 7, 1, 0, 0, 0),
    'postgres_conn_id': 'postgres_default'
}

sql = f"""
CREATE TABLE {table_variables['name']} (
  id BIGINT,
  Data_Indicador TIMESTAMP,
  Font VARCHAR(250),
  Frequencia_Indicador VARCHAR(25),
  Nom_Indicador VARCHAR(100),
  Nom_Variable VARCHAR(100),
  Territori VARCHAR(100),
  Unitat VARCHAR(100),
  Valor DECIMAL(16, 2),
  Insert_TS TIMESTAMP,
  CONSTRAINT pk_{table_variables['name']} PRIMARY KEY(id));
"""


def insert_rows():
    # cuidado con no ponerlo explícito!

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql_insert = f"""INSERT INTO {table_variables['name']}
                     VALUES (%s, %s, %s, %s, %s, %s ,%s, %s, %s, %s)"""

    http_hook = HttpHook(http_conn_id=table_variables['http_conn_id'],
                         method='GET')
    res = http_hook.run(endpoint=table_variables['endpoint'],
                        data={'resource_id': table_variables['resource_id'],
                              'limit': '10000000'})

    http_hook.check_response(response=res)

    bcn_covid_measures = res.json()['result']['records']

    bcn_covid_df = pd.DataFrame(bcn_covid_measures)
    bcn_covid_df = bcn_covid_df[['_id', 'Data_Indicador', 'Font', 'Frequencia_Indicador', 'Nom_Indicador',
                                 'Nom_Variable', 'Territori', 'Unitat', 'Valor']]
    bcn_covid_df.replace({'NA': np.nan,
                          '-Inf': np.nan,
                          'Inf': np.nan}, inplace=True)
    insert_ts = datetime.utcnow()

    for row in bcn_covid_df.itertuples(index=False):
        pg_hook.run(sql_insert, parameters=(row[0], row[1], row[2],
                                            row[3], row[4],
                                            row[5], row[6],
                                            row[7], row[8], insert_ts))


with DAG(dag_id='get_rep_covid_measures',
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
