#!/usr/bin/env python
# coding: utf-8

import os
import requests
import json
import pandas as pd
import sqlite3
from sqlalchemy import create_engine

# Loading environment variables
with open('variables.json') as file:
    variables = json.load(file)

# Where to get the data of the resource
url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=290eb517-e7fa-41fb-aa59-389becb8f55b&limit=100000000'

r = requests.get(url)
assert r.status_code == 200, "Request was not succesful, Status Code: {code}".format(code=r.status_code)


# Decapsulating the json object into a DataFrame object
bcn_covid_measures = r.json()['result']['records']
bcn_covid_df = pd.DataFrame(bcn_covid_measures)
bcn_covid_df = bcn_covid_df[['_id','Data_Indicador', 'Font', 'Frequencia_Indicador', 'Nom_Indicador',
                             'Nom_Variable', 'Territori', 'Unitat', 'Valor']].set_index('_id')

database_name = variables['sqlite_database_name']

# If the DDBB do not exist in our WD we create it
if database_name not in os.listdir():
    print('Creating Database object:')
    
    conn = sqlite3.connect(database_name)
    conn.close()
    
    assert database_name in os.listdir(), "Database could not be created."
    print(f"Database {database_name} created succesfully")
    
else:
    print(f"{database_name} already in directory")


table = variables['covid_measures_table']
engine = create_engine(f"sqlite:///{database_name}")

try:
    bcn_covid_df.to_sql(table,
                        engine,
                        if_exists='replace')
except Exception:
    raise Exception(f"Couldn't write pandas' DataFrame into table {table} of {database_name}")