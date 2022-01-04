import os
import pandas as pd
from tda import auth, client
from selenium import webdriver
import time
import datetime
import json
import csv
import pytz
import trading_calendars as tc


# please change to your tda credentials
token_path = 'token.json'
api_key = 'OUUAVDLG8ADCHZBAA2WWNR7RDBCAZINL@AMER.OAUTHAP'
redirect_uri = 'https://localhost:8080'
primary_account_id = 454180788
PATH = r"C:\Users\Gibraltar\Downloads\chromedriver.exe"

# TDA client
c = auth.easy_client(api_key, redirect_uri, token_path,
                     webdriver_func=lambda: webdriver.Chrome(PATH))


def afterhours(now=None):
    tz = pytz.timezone('US/Eastern')
    trading_cal = tc.get_calendar("XNYS")
    if not now:
        now = datetime.datetime.now(tz)
    openTime = datetime.time(hour=9, minute=30, second=0)
    closeTime = datetime.time(hour=16, minute=0, second=0)
    # If a holiday
    if trading_cal.is_session(now.strftime('%Y-%m-%d')) is False:
        return True
    # If before 0930 or after 1600
    # if (now.time() < openTime) or (now.time() > closeTime):
    #     return True
    # If it's a weekend
    if now.date().weekday() > 4:
        return True
    return False


# runs for 1d data, requires a ticker and filename input
def tda_append_1d(ticker, filename):
    r = c.get_price_history(ticker,
                            period_type=client.Client.PriceHistory.PeriodType.MONTH,
                            period=client.Client.PriceHistory.Period.ONE_DAY,
                            frequency_type=client.Client.PriceHistory.FrequencyType.DAILY,
                            frequency=client.Client.PriceHistory.Frequency.DAILY,
                            start_datetime=datetime.datetime.strptime('2021-11-15', '%Y-%m-%d'),
                            end_datetime=datetime.datetime.now())

    try:
        x = (json.dumps(r.json(), indent=4))
        df = pd.read_json(x)
        # if getting more than one data comment this out
        df = df['candles'].iloc[-1]
        df1 = {'ticker': ticker}
        df1.update(df)
        labels = ['ticker', 'datetime', 'open', 'high', 'low', 'close', 'volume']
        with open(filename, 'a', newline="") as f:
            writer = csv.DictWriter(f, fieldnames=labels)
            df1['datetime'] = datetime.datetime.fromtimestamp(df['datetime'] / 1000).strftime('%Y-%m-%d %H:%M')
            writer.writerow(df1)
    except ValueError:
        print('value error')
        pass
    except IndexError:
        print('index error')
        pass


def format_data(path):
    x = pd.read_csv(path)
    x.reset_index(drop=True, inplace=True)
    x = x.rename(columns={'Date': 'date'})
    x = x.reindex(['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'], axis='columns')
    x = x.set_index('ticker')
    x.to_csv(path)


#
# def tda_append_1h(ticker, filename):
#     r = c.get_price_history(ticker,
#                             period_type=client.Client.PriceHistory.PeriodType.MONTH,
#                             period=client.Client.PriceHistory.Period.ONE_DAY,
#                             frequency_type=client.Client.PriceHistory.FrequencyType.MINUTE,
#                             frequency=client.Client.PriceHistory.Frequency.EVERY_SIXTY_MINUTES)
#
#     try:
#         x = (json.dumps(r.json(), indent=4))
#         df = pd.read_json(x)
#         df = df['candles'].iloc[-1]
#         df1 = {'ticker': ticker}
#         df1.update(df)
#         labels = ['ticker', 'datetime', 'open', 'high', 'low', 'close', 'volume']
#         with open(filename, 'a', newline="") as f:
#             writer = csv.DictWriter(f, fieldnames=labels)
#             df1['datetime'] = datetime.datetime.fromtimestamp(df['datetime'] / 1000).strftime('%Y-%m-%d %H:%M')
#             writer.writerow(df1)
#     except ValueError:
#         print('value error')
#         pass
#     except IndexError:
#         print('index error')
#         pass
#
#
# def tda_append_5m(ticker, filename):
#     r = c.get_price_history(ticker,
#                             period_type=client.Client.PriceHistory.PeriodType.DAY,
#                             period=client.Client.PriceHistory.Period.YEAR_TO_DATE,
#                             frequency_type=client.Client.PriceHistory.FrequencyType.MINUTE,
#                             frequency=client.Client.PriceHistory.Frequency.EVERY_FIVE_MINUTES,
#                             end_datetime=datetime.datetime.now())
#
#     try:
#         x = (json.dumps(r.json(), indent=4))
#         df = pd.read_json(x)
#         df = df['candles'].iloc[-1]
#         labels = ['datetime', 'open', 'high', 'low', 'close', 'volume']
#         with open(filename, 'a', newline="") as f:
#             writer = csv.DictWriter(f, fieldnames=labels)
#             df['datetime'] = datetime.datetime.fromtimestamp(df['datetime'] / 1000).strftime('%Y-%m-%d %H:%M')
#             writer.writerow(df)
#     except ValueError:
#         pass
#
#
# def tda_append_15m(ticker, filename):
#     r = c.get_price_history(ticker,
#                             period_type=client.Client.PriceHistory.PeriodType.DAY,
#                             period=client.Client.PriceHistory.Period.YEAR_TO_DATE,
#                             frequency_type=client.Client.PriceHistory.FrequencyType.MINUTE,
#                             frequency=client.Client.PriceHistory.Frequency.EVERY_FIFTEEN_MINUTES,
#                             end_datetime=datetime.datetime.now())
#
#     try:
#         x = (json.dumps(r.json(), indent=4))
#         df = pd.read_json(x)
#         df = df['candles'].iloc[-1]
#         labels = ['datetime', 'open', 'high', 'low', 'close', 'volume']
#         with open(filename, 'a', newline="") as f:
#             writer = csv.DictWriter(f, fieldnames=labels)
#             df['datetime'] = datetime.datetime.fromtimestamp(df['datetime'] / 1000).strftime('%Y-%m-%d %H:%M')
#             writer.writerow(df)
#     except ValueError:
#         pass
#
#
# def tda_append_30m(ticker, filename):
#     r = c.get_price_history(ticker,
#                             period_type=client.Client.PriceHistory.PeriodType.DAY,
#                             period=client.Client.PriceHistory.Period.YEAR_TO_DATE,
#                             frequency_type=client.Client.PriceHistory.FrequencyType.MINUTE,
#                             frequency=client.Client.PriceHistory.Frequency.EVERY_THIRTY_MINUTES,
#                             end_datetime=datetime.datetime.now())
#
#     try:
#         x = (json.dumps(r.json(), indent=4))
#         df = pd.read_json(x)
#         df = df['candles'].iloc[-1]
#         labels = ['datetime', 'open', 'high', 'low', 'close', 'volume']
#         with open(filename, 'a', newline="") as f:
#             writer = csv.DictWriter(f, fieldnames=labels)
#             df['datetime'] = datetime.datetime.fromtimestamp(df['datetime'] / 1000).strftime('%Y-%m-%d %H:%M')
#             writer.writerow(df)
#     except ValueError:
#         pass


# my directory for the data to loop through
directory = rf'\\TOWER\financial data\1d Data Stocks'


# if you have the afterhours function, call it and uncomment line 157
def append():
    start = time.time()
    counter = 0
    for filename in os.listdir(directory):
        if counter % 50 == 0:
            time.sleep(30)
        # change the file extension to the one that is required
        if filename.endswith('1d.csv'):
            ticker = filename.split("_")[0]
            tda_append_1d(ticker, directory + r"\\" + filename)
            print(f'Im done {ticker}')
        counter += 1
    end = time.time() - start
    print(f'{end / 60} minutes to run.')


if afterhours() is False:
    append()
