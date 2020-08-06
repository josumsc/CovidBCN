import requests
import pandas as pd
from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'Josu Alonso',
    'start_date': datetime(2020, 7, 1, 0, 0, 0)
}


def get_request():
    r = requests.get(url)
    assert r.status_code == 200, "Request was not succesful, Status Code: {code}".format(code=r.status_code)

    bcn_covid_measures = r.json()['result']['records']
    bcn_covid_df = pd.DataFrame(bcn_covid_measures)
    bcn_covid_df = bcn_covid_df[['_id','Data_Indicador', 'Font', 'Frequencia_Indicador', 'Nom_Indicador',
                                 'Nom_Variable', 'Territori', 'Unitat', 'Valor']].set_index('_id')
    return bcn_covid_df


with DAG(dag_id='get_rep_covid_measures',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    start = DummyOperator(task_id='start')
    
    request = PythonOperator(task_id='request',
                             python_callable=get_request)

start >> request
