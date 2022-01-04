import pandas as pd
import os
from yahooquery import Ticker
import pathlib
import time

directory = r'\\TOWER\financial data\1d Data Stocks'


def main():
    append()


def format_data(path):
    x = pd.read_csv(path)
    x.reset_index(drop=True, inplace=True)
    x = x.rename(columns = {'Date':'date'})
    x = x.reindex(['ticker','date','open','high','low','close','volume'], axis='columns')
    x = x.set_index('ticker')
    x.to_csv(path)


def append_1d(ticker, days='1d'):
    x = Ticker(ticker, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = rf'\\TOWER\financial data\1d Data Stocks\{ticker}_1d.csv'
    format_data(path)
    if pathlib.Path(path).exists():
        with open(path, 'a', newline="") as f:
            data = x.history(period=days, interval='1d')
            try:
                df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
                df.to_csv(f, header=False)
                # print(df)
                f.close()
            except TypeError:
                pass
    else:
        data = x.history(period='max', interval='1d')
        df = pd.DataFrame(data[['open','high','low','close','volume']])
        df.to_csv(path)


def append():
    start = time.perf_counter()
    x = 0
    for filename in os.listdir(directory):
        x += 1
        if filename.endswith('.csv'):
            ticker = filename[:-7]
            append_1d(ticker)
            print(f'Data Appended {ticker}')
        if x % 100 == 0:
            print('\nTime to sleep, Goodnight...')
            time.sleep(5)
            print('Done sleeping...\n')
    finish = time.perf_counter()
    print(f'That took {(round(finish - start, 2)/60)} minutes to append')


# Get number of files in a directory
# path, dirs, files = next(os.walk(r"\\TOWER\financial data\1d Data Stocks"))
# file_count = len(files)


if __name__ == "__main__":
    main()
