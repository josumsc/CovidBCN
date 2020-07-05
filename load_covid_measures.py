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

database_name = variables['sqlite_database_name']
table_name = variables['covid_measures_table']['name']
url = variables['covid_measures_table']['url']

def get_request(url):
    """
    Fulfills a HTTP request to fetch the data from the API
    :param url: URL of the resource to get
    :return: Request object with the data fetched
    """
    r = requests.get(url)
    assert r.status_code == 200, "Request was not succesful, Status Code: {code}".format(code=r.status_code)
    return r


# Decapsulating the json object into a DataFrame object
def request_to_df(r):
    """
    Transforms our request into a Pandas' DataFrame object
    :param r: Request object with the data
    :return: DataFrame with the fetched data
    """
    bcn_covid_measures = r.json()['result']['records']
    bcn_covid_df = pd.DataFrame(bcn_covid_measures)
    bcn_covid_df = bcn_covid_df[['_id','Data_Indicador', 'Font', 'Frequencia_Indicador', 'Nom_Indicador',
                                 'Nom_Variable', 'Territori', 'Unitat', 'Valor']].set_index('_id')
    return bcn_covid_df


# If the DDBB do not exist in our WD we create it
def is_database_in_wd(database_name):
    """
    Checks if the database exists in our working directory, and if not creates it
    :param database_name: Database to look for
    :return: None
    """
    if database_name not in os.listdir():
        print('Creating Database object:')

        conn = sqlite3.connect(database_name)
        conn.close()

        assert database_name in os.listdir(), "Database could not be created."
        print(f"Database {database_name} created succesfully")
    else:
        print(f"{database_name} already in directory")

    return None

def dataframe_to_sql(df,database_name,table_name):
    """
    Writes our DataFrame to a table in our SQL Database
    :param df: DataFrame to import in SQL
    :param database_name: Database in which we should dump the data
    :param table_name: Name of the table to dump the data into
    :return: None
    """
    engine = create_engine(f"sqlite:///{database_name}")

    try:
        df.to_sql(table_name,
                  engine,
                  if_exists='replace')
    except Exception:
        raise Exception(f"Couldn't write pandas' DataFrame into table {table_name} of {database_name}")
    return None

def main():
    """
    Main function of the script. Combines previous steps.
    :return: None
    """
    r = get_request(url)
    df = request_to_df(r)
    is_database_in_wd(database_name)
    dataframe_to_sql(df,database_name,table_name)
    return None

if __name__ == '__main__':
    main()