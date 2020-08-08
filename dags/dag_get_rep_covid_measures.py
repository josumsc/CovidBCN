from datetime import datetime
import pandas as pd

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook


table_variables = Variable.get('covid_measures_table',
                               deserialize_json=True)

default_args = {
    'owner': 'Josu Alonso',
    'start_date': datetime(2020, 7, 1, 0, 0, 0),
    'postgres_conn_id': 'postgres_covid'
}

sql = f"""
CREATE TABLE {table_variables['name']} (
id INT,
Data_Indicador TIMESTAMP,
Font VARCHAR(100)),
Frequencia_Indicador VARCHAR(25),
Nom_Indicador VARCHAR(55),
Nom_Variable VARCHAR(55),
Territori VARCHAR(55),
Unitat VARCHAR(55),
Valor INT,
Insert_TS TIMESTAMP,
CONTRAINT pk_{table_variables['name']} PRIMARY KEY (id);
"""


def insert_rows(postgres_conn_id):
    # cuidado con no ponerlo explícito!

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    sql_insert = f"""INSERT INTO {table_variables['name']}
                     VALUES (%s, %s, %s, %s, %s, %s ,%s, %s, %s, %s)"""

    http_hook = HttpHook(http_conn_id=table_variables['url'],
                         method='GET')
    res = http_hook.run('')

    bcn_covid_measures = res.json()['result']['records']

    bcn_covid_df = pd.DataFrame(bcn_covid_measures)
    bcn_covid_df = bcn_covid_df[['_id', 'Data_Indicador', 'Font', 'Frequencia_Indicador', 'Nom_Indicador',
                                 'Nom_Variable', 'Territori', 'Unitat', 'Valor']]
    insert_ts = datetime.utcnow()

    for _id, date, source, freq, ind, var, region, unit, value in bcn_covid_df.iteritems():
        pg_hook.run(sql_insert, parameters=(_id, date, source, freq, ind, var, region, unit, value, insert_ts))


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
