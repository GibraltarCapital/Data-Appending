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


def append_1h(ticker, days='1d'):
    ticker_temp = ticker.replace('_', '-')
    x = Ticker(ticker_temp, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = r"D:\Crypto_Data\1h_data\\" + ticker + r"_1h.csv"
    if pathlib.Path(path).exists():
        format_data(path)
        with open(path, 'a', newline="") as f:
            data = x.history(period=days, interval='1h')
            df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
            df.to_csv(f, header=False)
            print(f)
            f.close()
    else:
        data = x.history(period='6mo', interval='1h')
        df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
        df.to_csv(path)


append_1h('BTC_USD')
append_1h('ETH_USD')
append_1h('ETC_USD')
append_1h('DASH_USD')
append_1h('ALGO_USD')
append_1h('LTC_USD')
append_1h('ADA_USD')
append_1h('XLM_USD')
append_1h('LINK_USD')
append_1h('XMR_USD')
append_1h('DOGE_USD')
append_1h('BNB_USD')
