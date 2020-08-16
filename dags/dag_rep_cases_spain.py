from datetime import datetime
import pandas as pd

from airflow.models import DAG
from airflow.models import Variable
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook

table_variables = Variable.get('cases_spain_table',
                               deserialize_json=True)

sql = f"""
CREATE TABLE {table_variables['name']}(
    cases_date DATE,
    cases_confirmed INT,
    cases_icu INT,
    cases_dead INT,
    cases_hospitalized INT,
    cases_recovered INT,
    cases_confirmed_daily INT,
    cases_icu_daily INT,
    cases_dead_daily INT,
    cases_hospitalized_daily INT,
    cases_recovered_daily INT,
    insert_ts TIMESTAMP,
    CONSTRAINT pk_{table_variables['name']} PRIMARY KEY(cases_date));
"""

default_args = {
    'owner': 'Josu Alonso',
    'start_date': dates.days_ago(1),
    'postgres_conn_id': 'postgres_default'
}


def insert_rows():

    insert_ts = datetime.utcnow()

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql_insert = f"""INSERT INTO {table_variables['name']}
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    http_hook = HttpHook(http_conn_id=table_variables['http_conn_id'],
                         method='GET')
    res = http_hook.run(endpoint=table_variables['endpoint'])
    http_hook.check_response(response=res)

    cases_df = pd.DataFrame(res.json()['timeline'])
    cases_df['fecha'] = pd.to_datetime(cases_df['fecha'])

    for row in cases_df.itertuples(index=False):
        date = row.fecha

        information = pd.Series(row.regiones[0]['data'])

        information = information[['casosConfirmados', 'casosUci', 'casosFallecidos', 'casosHospitalizados', 'casosRecuperados',
                                   'casosConfirmadosDiario', 'casosUciDiario', 'casosFallecidosDiario',
                                   'casosHospitalizadosDiario', 'casosRecuperadosDiario']]
        pg_hook.run(sql_insert, parameters=(date, information[0], information[1],
                                            information[2], information[3], information[4],
                                            information[5], information[6], information[7],
                                            information[8], information[9], insert_ts))


with DAG(dag_id='rep_cases_spain',
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
