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
from configparser import ConfigParser


def _parse_arguments():
    parser = argparse.ArgumentParser(description="Run trade data for specific country")
    parser.add_argument("-countries", "--list", type=str, dest='list', required=True, help="The country name to be run")
    parser.add_argument("-year", type=str, required=False, help="year of data to be loaded")
    parser.add_argument("-month", type=str, required=False, help="month of data, if not supplied, then all months")
    return parser.parse_args()


def process_trade_table(cur, country_dict, country_name, year, month=None, cc=None):
    """
    Process monthly data for selected country
    :param cur: cursor for SQL database
    :param country_dict: dictionary to look up country code for API call
    :param country_name: name of country
    :param year: year for the monthly dataset
    :param month
    :param cc: classification codes
    :return:
    """
    def api_call(year, country_code, cc):
        # wait for time between api calls
        time.sleep(8)
        # get data as a json from API call and transform to pandas
        url = 'https://comtrade.un.org/api/get?type=C&freq=M&px=HS&ps={year}&r={country}&p=0&rg=all&cc={cc}'.format(
            year=year, country=country_code, cc=cc)
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

    try:
        country_code = country_dict[country_name]
    except:
        print("Country {} is an invalid argument".format(country_name))
        return

    if not cc:
        # if no classification is defined, pull all the codes for 2 digit AG2
        cc = "AG2"
    if not year:
        #need to loop between 2019 and 2020
        years = [2019,2020]
    else:
        years = [year]

    for y in years:
        api_call(y, country_code, cc)


def process_covid_cases(cur, month, country_lookup_dict):
    """
    Process covid cases for all countries
    :param cur: cursor
    :param month: month
    :param country_lookup_dict: country name lookup dictionary
    :return:
    """
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
    config = ConfigParser()
    config.read("config.ini")
    countries = [item for item in args.list.split(",")]

    conn = psycopg2.connect(host=config.get('local', 'host'), dbname="comtrade")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create lookup dict for country code
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
