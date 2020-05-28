import time

import pandas as pd
import psycopg2
import requests
from pandas.io.json import json_normalize
import numpy as np

from sql_queries import *


def process_dimension_tables(cur, filepath, query, table_name):
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


def process_trade_table(cur, country_dict, country_name, year):
    """
    Process monthly data for selected country
    :param cur: cursor for SQL database
    :param country_dict: dictionary to look up country code for API call
    :param country_name: name of country
    :param year: year for the monthly dataset
    :return:
    """
    time.sleep(8)  # selected number of seconds to wait to make API robust
    country_code = country_dict[country_name]

    # get data as a json from API call and transform to pandas
    url = 'https://comtrade.un.org/api/get?type=C&freq=M&px=HS&ps={year}&r={country}&p=0&rg=all&cc=01,02,03,04,05,06,07,08,09,10'.format(
        year=year, country=country_code)
    un_data = requests.get(url)
    json = un_data.json()
    df = json_normalize(json['dataset'])

    # process data and move to Postgres database
    if df.empty:
        print("Monthly data not available for {country} during time period {p}".format(country=country_name, p=year))
    else:
        df = df[['rtCode', 'ptCode', 'cmdCode', 'period', 'rgDesc', 'TradeQuantity', 'TradeValue']]
        df['period'] = pd.to_datetime(df['period'], format="%Y%m")
        df['TradeQuantity'] = df['TradeQuantity'].fillna(0)
        df['TradeValue'] = df['TradeValue'].fillna(0)
        for i, row in df.iterrows():
            cur.execute(insert_trades, list(row))
        print("Processed monthly data for {country} during time period {p}".format(country=country_name, p=year))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=comtrade")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    process_dimension_tables(cur, 'https://comtrade.un.org/Data/cache/tradeRegimes.json',
                             insert_import_export, "import_export")
    process_dimension_tables(cur, 'https://comtrade.un.org/Data/cache/partnerAreas.json',
                             insert_countries, "countries")
    process_dimension_tables(cur, 'https://comtrade.un.org/Data/cache/classificationHS.json',
                             insert_classification_codes, "classifications")
    # create lookup dict for country code
    country_df = pd.read_sql("""SELECT country_id, country_name
                                FROM countries """, conn)
    country_lookup_dict = dict(zip(country_df['country_name'], country_df['country_id']))
    for country in ['China', 'USA', 'Canada']: #,'Switzerland', 'Italy', 'Australia']:
        process_trade_table(cur, country_lookup_dict, country, 2020)
    conn.close()


if __name__ == '__main__':
    main()
