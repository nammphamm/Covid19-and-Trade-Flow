import time

import pandas as pd
import psycopg2
import requests
from pandas.io.json import json_normalize
import numpy as np
from pandas.tseries.offsets import MonthEnd
import datetime

from sql_queries import *

import argparse


def _parse_arguments():
    parser = argparse.ArgumentParser(description="Run trade data for specific country")
    parser.add_argument("-countries", "--list", type=str, dest='list', required=True, help="The country name to be run")
    parser.add_argument("-year", type=str, required=True, help="year of data to be loaded")
    parser.add_argument("-month", type=str, required=False, help="month of data, if not supplied, then all months")
    return parser.parse_args()


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


def process_trade_table(cur, country_dict, country_name, year, month=None):
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

    if not month:
        month = ",".join(["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"])

    # get data as a json from API call and transform to pandas
    url = 'https://comtrade.un.org/api/get?type=C&freq=M&px=HS&ps={year}&r={country}&p=0&rg=all&cc={month}'.format(
        year=year, country=country_code, month=month)
    un_data = requests.get(url)
    json = un_data.json()
    df = json_normalize(json['dataset'])

    # process data and move to Postgres database
    if df.empty:
        print("Monthly data not available for {country} during time period {p}".format(country=country_name, p=year))
    else:
        df = df[['rtCode', 'ptCode', 'cmdCode', 'period', 'rgDesc', 'TradeQuantity', 'TradeValue']]
        df['period'] = pd.to_datetime(df['period'], format="%Y%m") + MonthEnd(1)
        df['TradeQuantity'] = df['TradeQuantity'].fillna(0)
        df['TradeValue'] = df['TradeValue'].fillna(0)
        for i, row in df.iterrows():
            cur.execute(insert_trades, list(row))
        print("Processed monthly trades data for {country} during time period {p}".format(country=country_name, p=year))


def process_covid_cases(cur, month, country_lookup_dict):
    date_string = datetime.date(2020, month, 1) + MonthEnd(1)
    try:
        df = pd.read_csv(
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{}.csv".format(date_string.strftime("%m-%d-%Y")),
            sep=',')
    except:
        print("Covid data for all countries not available for {}".format(date_string))
        return

    # preprocess downloaded covid data
    if "Country/Region" in list(df):
        df = df.rename({"Country/Region": "Country_Region"}, axis=1)
    if "Active" not in list(df):
        df["Active"] = df["Confirmed"] - df["Deaths"] - df["Recovered"]
    countries_dict = {"UK": "United Kingdom", "US": "USA", "Mainland China": "China"}
    df["Country_Region"] = np.where(df["Country_Region"].isin(countries_dict), df["Country_Region"].map(countries_dict),
                                    df["Country_Region"])
    df['period'] = date_string

    # group by and sum up cases by country
    df["Country_Region"] = df["Country_Region"].map(country_lookup_dict)
    df = df.groupby(["Country_Region", "period"])['Confirmed', 'Deaths', 'Recovered', 'Active'].sum().reset_index()

    # process data and move to Postgres database
    df = df[['Country_Region', 'period', 'Confirmed', 'Deaths', 'Recovered', 'Active']]

    for i, row in df.iterrows():
        cur.execute(insert_cases, list(row))
    print("Processed monthly covid data for all countries during time period {}".format(date_string))


def main():
    args = _parse_arguments()
    countries = [item for item in args.list.split(",")]

    conn = psycopg2.connect("host=127.0.0.1 dbname=comtrade")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    process_dimension_tables(cur, 'https://comtrade.un.org/Data/cache/tradeRegimes.json',
                             insert_import_export, "import_export")
    process_dimension_tables(cur, 'https://comtrade.un.org/Data/cache/partnerAreas.json',
                             insert_countries, "countries")
    process_dimension_tables(cur, 'https://comtrade.un.org/Data/cache/classificationHS.json',
                             insert_classification_codes, "classifications")

    #create lookup dict for country code
    country_df = pd.read_sql("""SELECT country_id, country_name
                                FROM countries """, conn)
    country_lookup_dict = dict(zip(country_df['country_name'], country_df['country_id']))

    # process covid cases for all countries
    for month in range(1, 13):
        process_covid_cases(cur, month, country_lookup_dict)

    # process trades data for some countries
    for country in countries:
        process_trade_table(cur, country_lookup_dict, country, args.year, args.month)

    conn.close()


if __name__ == '__main__':
    main()
