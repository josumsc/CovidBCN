from datetime import datetime

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


table_variables = Variable.get('traffic_measures_table',
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
