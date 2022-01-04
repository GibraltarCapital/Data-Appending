from yahooquery import Ticker
import pandas as pd
import pathlib
import datetime


def format_data(path):
    x = pd.read_csv(path)
    x.reset_index(drop=True, inplace=True)
    x = x.rename(columns={'Date': 'date'})
    x = x.reindex(columns=['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'])
    x = x.set_index('ticker')
    print(x)
    x.to_csv(path)


def append_1d(ticker, days='1d'):
    ticker_temp = ticker.replace('_', '-')
    x = Ticker(ticker_temp, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = r"\\TOWER\financial data\1d Data Crypto\\" + ticker + r"_1d.csv"
    if pathlib.Path(path).exists():
        format_data(path)
        with open(path, 'a', newline="") as f:
            data = x.history(period=days, interval='1d')
            df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
            df.to_csv(f, header=False)
            df.ticker = ticker
            print(f)
            f.close()
    else:
        data = x.history(period='max', interval='1d')
        df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
        df.to_csv(path)


append_1d('BTC_USD')
append_1d('ETH_USD')
append_1d('ETC_USD')
append_1d('DASH_USD')
append_1d('ALGO_USD')
append_1d('LTC_USD')
append_1d('ADA_USD')
append_1d('XLM_USD')
append_1d('LINK_USD')
append_1d('XMR_USD')
append_1d('DOGE_USD')
append_1d('BNB_USD')