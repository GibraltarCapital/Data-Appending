import os
import pathlib
import warnings
import pandas as pd
from yahooquery import Ticker
import time
warnings.filterwarnings('ignore')

directory = r'\\TOWER\financial data\15m Data Stocks'


def format_data(path):
    x = pd.read_csv(path)
    x.reset_index(drop=True, inplace=True)
    x = x.rename(columns = {'Date':'date'})
    x = x.reindex(['ticker','date','open','high','low','close','volume'], axis='columns')
    x = x.set_index('ticker')
    x.to_csv(path)


def append_15m(ticker, days = '1d'):
    x = Ticker(ticker, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = rf"\\TOWER\financial data\15m Data Stocks\{ticker}_15m.csv"
    format_data(path)
    if pathlib.Path(path).exists():
        with open(path, 'a', newline="") as f:
            data = x.history(period=days, interval='15m')
            try:
                df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
                df.to_csv(f, header=False)
                # print(f)
                f.close()
            except TypeError:
                pass
    else:
        data = x.history(period='60d', interval='15m')
        df = data[['open','high','low','close','volume']]
        df.to_csv(path)


def append():
    start = time.perf_counter()
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            ticker = filename[:-8]
            append_15m(ticker)
            print(f'Data Appended {ticker}')
    finish = time.perf_counter()
    print(f'That took {round(finish - start, 2)} seconds to append')


append()