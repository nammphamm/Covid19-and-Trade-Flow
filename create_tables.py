import psycopg2

from sql_queries import *
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
from configparser import ConfigParser


def create_database(config):
    """
    Create database and return new connection string
    :return:
    """
    # connect to default database
    conn = psycopg2.connect(host=config.get('local', 'host'), dbname="postgres")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    # create database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS comtrade")
    cur.execute("CREATE DATABASE comtrade WITH ENCODING 'utf8'")
    print("Database created")

    # close connection
    conn.close()

    # connect to database just created
    conn = psycopg2.connect(host=config.get('local', 'host'), dbname="comtrade")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    # drop tables in case we need to start fresh
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables as defined
    :return:
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def process_dimension_tables(cur, filepath, query, table_name):
    """
    Process dimension tables (countries, classifications)
    :param cur: cursor
    :param filepath: file path of reference files
    :param query: query to be executed
    :param table_name: table to be updated
    :return: None
    """
    def process_countries_tables(indicator_path, countries_path, countries_table):
        indicators = pd.read_excel(indicator_path)
        indicators = indicators.drop(['Country Name', 'Time', 'Time Code'], axis=1)
        indicators.columns = ['ISO3-digit Alpha', '2017_GDP', '2017 GDP per capita','2017 Population',
                      '2017 GDP per capita growth', '2017 GDP per capita, PPP', '2017 GDP, PPP',
                      '2017 GDP growth', '2017 GDP (current LCU)', '2017 GDP per capita (constant LCU)',
                      '2017 General government final consumption expenditure', '2017 Military expenditure']

        countries = pd.read_excel(countries_path)
        countries = countries[countries['End Valid Year'] == "Now"]
        countries = countries[['Country Code', 'ISO3-digit Alpha']]
        joined = pd.merge(countries, indicators, on="ISO3-digit Alpha")
        joined['Country Code'] = joined['Country Code'].astype(str)
        countries_table = countries_table.merge(joined, left_on="id", right_on="Country Code", how="left")
        for col in countries_table:
            countries_table.loc[countries_table[col] == '..', col] = np.NaN
        return countries_table[['id', 'text', '2017_GDP', '2017 GDP per capita',
                                '2017 Population', '2017 Military expenditure']]

    json = pd.read_json(filepath, orient='records')
    df = json_normalize(json['results'])
    if table_name == "classifications":
        # only include the summary group
        df[df.parent.isin(['TOTAL', "#"])]
    elif table_name == 'countries':
        df = process_countries_tables("reference_data/Population and GDP by Country.xlsx",
                                      "reference_data/Comtrade Country Code and ISO list.xlsx",
                                      df)
    for i, row in df.iterrows():
        cur.execute(query, list(row))
    print("Processed {}".format(table_name))


def main():
    config = ConfigParser()
    config.read("config.ini")
    cur, conn = create_database(config)
    conn.set_session(autocommit=True)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    process_dimension_tables(cur, 'https://comtrade.un.org/Data/cache/tradeRegimes.json',
                             insert_import_export, "import_export")
    process_dimension_tables(cur, 'https://comtrade.un.org/Data/cache/partnerAreas.json',
                             insert_countries, "countries")
    process_dimension_tables(cur, 'https://comtrade.un.org/Data/cache/classificationHS.json',
                             insert_classification_codes, "classifications")
    conn.close()


if __name__ == "__main__":
    main()
