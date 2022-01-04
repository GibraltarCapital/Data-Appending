from yahooquery import Ticker
import pandas as pd
import pathlib
import datetime
import pytz
import exchange_calendars as ec


def after_hours(now=None):
    tz = pytz.timezone('US/Eastern')
    trading_cal = ec.get_calendar("XNYS")
    if not now:
        now = datetime.datetime.now(tz)
    # If a holiday
    if trading_cal.is_session(now.strftime('%Y-%m-%d')) is False:
        return True
    if now.date().weekday() > 4:
        return True
    return False


def format_data(path):
    x = pd.read_csv(path)
    x.reset_index(drop=True, inplace=True)
    x = x.rename(columns = {'Date':'date'})
    x = x.reindex(['ticker','date','open','high','low','close','volume'], axis='columns')
    x = x.set_index('ticker')
    x.to_csv(path)


def append_15m(ticker, days = '1d'):
    x = Ticker(ticker, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = rf"C:\Users\mceli\PycharmProjects\Financial-Algorithms\15m_data\{ticker}_15m.csv"
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


def append_30m(ticker, days='1d'):
    x = Ticker(ticker, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = rf"C:\Users\mceli\PycharmProjects\Financial-Algorithms\30m_data\{ticker}_30m.csv"
    format_data(path)
    if pathlib.Path(path).exists():
        with open(path, 'a', newline="") as f:
            data = x.history(period=days, interval='30m')
            try:
                df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
                df.to_csv(f, header=False)
                # print(f)
                f.close()
            except TypeError:
                pass
    else:
        data = x.history(period='3mo', interval='30m')
        df = pd.DataFrame(data[['open','high','low','close','volume']])
        df.to_csv(path)


def append_5m(ticker, days='1d'):
    x = Ticker(ticker, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = rf"C:\Users\mceli\PycharmProjects\Financial-Algorithms\5m_data\{ticker}_5m.csv"
    format_data(path)
    if pathlib.Path(path).exists():
        with open(path, 'a', newline="") as f:
            data = x.history(period=days, interval='5m')
            try:
                df = pd.DataFrame(data[['open','high','low','close','volume']])
                df.to_csv(f, header=False)
                # print(f)
                f.close()
            except TypeError:
                pass
    else:
        data = x.history(period='1mo', interval='5m')
        df = pd.DataFrame(data[['open','high','low','close','volume']])
        df.to_csv(path)


def append_1m(ticker, days='1d'):
    x = Ticker(ticker, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = rf"C:\Users\mceli\PycharmProjects\Financial-Algorithms\1m_data\{ticker}_1m.csv"
    format_data(path)
    if pathlib.Path(path).exists():
        with open(path, 'a', newline="") as f:
            data = x.history(period=days, interval='1m')
            try:
                #data = data.loc[pd.IndexSlice[:, '2020-12-09 09:30:00':], :]
                try:
                    df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
                    df.to_csv(f, header=False)
                    # print(f)
                    f.close()
                except TypeError:
                    pass
            except AttributeError:
                pass

    else:
        data = x.history(period='7d', interval='1m')
        df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
        df.to_csv(path)


def append_1h(ticker, days='1d'):
    x = Ticker(ticker, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = rf"C:\Users\mceli\PycharmProjects\Financial-Algorithms\1h_data\{ticker}_1h.csv"
    format_data(path)
    if pathlib.Path(path).exists():
        with open(path, 'a', newline="") as f:
            data = x.history(period=days, interval='1h')
            try:
                df = pd.DataFrame(data[['open', 'high', 'low', 'close', 'volume']])
                df.to_csv(f, header=False)
                # print(f)
                f.close()
            except TypeError:
                pass
    else:
        data = x.history(period='3mo', interval='1h')
        df = pd.DataFrame(data[['open','high','low','close','volume']])
        df.to_csv(path)


def append_1d(ticker, days='1d'):
    x = Ticker(ticker, retry=20, status_forcelist=[404, 429, 500, 502, 503, 504])
    path = rf"\\TOWER\financial data\1d Data Stocks{ticker}_1d.csv"
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