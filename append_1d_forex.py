# from Frameworks.append_stock_data import *
import pandas as pd
import os
from yahooquery import Ticker
import pathlib
import trading_calendars as tc
import pytz
import datetime

directory = r'\\TOWER\financial data\1d Data FOREX'


def format_data(path):
    x = pd.read_csv(path)
    x.reset_index(drop=True, inplace=True)
    x = x.rename(columns={'Date': 'date'})
    x = x.reindex(['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'], axis='columns')
    x = x.set_index('ticker')
    x.to_csv(path)


def append_1d(ticker, days='5d'):
    x = Ticker(ticker, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = rf'D:\Forex_Market_Data\1d\{ticker}_1d.csv'
    format_data(path)
    if pathlib.Path(path).exists():
        with open(path, 'a', newline="") as f:
            data = x.history(start='2021-10-03', interval='1d')
            try:
                df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
                df.to_csv(f, header=False)
                # print(df)
                f.close()
            except TypeError:
                pass
    else:
        data = x.history(period='max', interval='1d')
        df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
        df.to_csv(path)


def append():
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            ticker = filename[:-7]
            append_1d(ticker)
            print(f'Im done {ticker}')


append()
