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


# runs for 1d data, requires a ticker and filename input
def format_data(path):
    x = pd.read_csv(path)
    x.reset_index(drop=True, inplace=True)
    x = x.rename(columns={'Date': 'date'})
    x = x.reindex(['ticker', 'date', 'open', 'high', 'low', 'close', 'volume'], axis='columns')
    x = x.set_index('ticker')
    x.to_csv(path)


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


def tda_append_30m(ticker, filename):
    # r = c.get_price_history('AAPL',
    #                         period_type=client.Client.PriceHistory.PeriodType.YEAR,
    #                         period=client.Client.PriceHistory.Period.ONE_YEAR,
    #                         frequency_type=client.Client.PriceHistory.FrequencyType.MINUTE,
    #                         frequency=client.Client.PriceHistory.Frequency.EVERY_FIFTEEN_MINUTES)
    r = c.get_price_history_every_thirty_minutes(ticker, start_datetime=datetime.datetime.now(),
                                                 end_datetime=datetime.datetime.now())
    try:
        x = (json.dumps(r.json(), indent=4))
        df = pd.read_json(x)
        # if getting more than one data comment this out
        #df = df['candles'].iloc[-1]
        df1 = {'ticker': ticker}
        df1.update(df)
        labels = ['ticker', 'datetime', 'open', 'high', 'low', 'close', 'volume']
        with open(filename, 'a', newline="") as f:
            writer = csv.DictWriter(f, fieldnames=labels)
            for i in df1['candles']:
                i['ticker'] = ticker
                i['datetime'] = datetime.datetime.fromtimestamp(i['datetime'] / 1000).strftime('%Y-%m-%d %H:%M')
                writer.writerow(i)

    except ValueError:
        print('value error')
        pass
    except IndexError:
        print('index error')
        pass


directory = rf'\\TOWER\financial data\30m Data Stocks'


def append():
    # x = afterhours()
    start = time.time()
    # if x is False:
    counter = 0
    for filename in os.listdir(directory):
        if counter % 50 == 0:
                            time.sleep(40)
        # change the file extension to the one that is required
        if filename.endswith('30m.csv'):
            ticker = filename.split("_")[0]
            #data = pd.read_csv(directory + r"\\" + filename)
            #data.set_index('ticker', inplace=True)
            #data = data.iloc[:-2 , :]
            #data.to_csv(directory + r"\\" + filename)
            tda_append_30m(ticker, directory + r"\\" + filename)
            print(f'Im done {ticker}')
        counter += 1
    end = time.time() - start
    print(f'{end / 60} minutes to run.')


# if you have the afterhours function, call it and uncomment line 157

if afterhours() is False:
    append()
