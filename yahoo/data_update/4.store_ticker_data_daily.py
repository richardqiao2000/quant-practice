import urllib.request
import ssl
import requests
import time
from datetime import datetime
import psycopg2
import pandas as pd
import sqlalchemy

# Set start/end day to update data
day_start = "2021-02-22"
day_end = "2021-02-23"
last_ticker = None  # to continue from last job executed ticker

# get all ticker name and id
try:
  sql = "select * from tickers order by name"
  engine = sqlalchemy.create_engine('postgresql://rqiao2:@localhost/stock_data')
  df_tickers = pd.read_sql(sql, con=engine)
except(Exception, psycopg2.DatabaseError) as error:
  print(error)


# get data from yahoo and save into local database
utc_time = datetime.strptime(day_start + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
day_start_epoch_time = round((utc_time - datetime(1970, 1, 1)).total_seconds())

utc_time = datetime.strptime(day_end + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
day_end_epoch_time = round((utc_time - datetime(1970, 1, 1)).total_seconds())

list_ticker = df_tickers['name']
tickers_data_table = "tickers_data"

conn = None
attemp = 0
try:
  conn = psycopg2.connect("dbname=stock_data user=rqiao2 password=")
  cur = conn.cursor()
  i = 0
  end_index = len(list_ticker)
  while last_ticker and list_ticker[i] != last_ticker:
    i = i + 1
  while i < end_index:
    ticker = list_ticker[i]
    print(ticker)

    # 2) Get data from yahoo service
    list_daily_data = []
    try:
      url = "https://query1.finance.yahoo.com/v7/finance/download/" \
            + ticker + "?period1=" + str(day_start_epoch_time) + "&period2=" + str(day_end_epoch_time) + "&interval=1d&events=history&includeAdjustedClose=true"
      # request and download file
      ssl._create_default_https_context = ssl._create_unverified_context
      response = urllib.request.urlopen(url)
      data = response.read()      # a `bytes` object
      text = data.decode('utf-8')
      list_daily_data = text.split("\n")
      attemp = 0
    except requests.exceptions.ProxyError as error:
      print(error)
      print(url)
      i = i + 1
      if attemp == 2:
        attemp = 0
        i = i + 1
      else:
        print("retrying ...")
        time.sleep(10)
      continue
    except Exception as error:
      print(error)
      print(url)
      attemp += 1
      if attemp == 2:
        attemp = 0
        i = i + 1
      else:
        print("retrying ...")
        time.sleep(10)
      continue

    # 3) save stock price into database
    try:
      ticker_id = df_tickers[lambda x: x['name'] == ticker]['id'].tolist()[0]
      for i_daily in range(1, len(list_daily_data)):
        line = list_daily_data[i_daily]
        arr = line.split(",")
        utc_time = datetime.strptime(arr[0] + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
        epoch_time = round((utc_time - datetime(1970, 1, 1)).total_seconds())
        val_time = epoch_time
        val_open = arr[1]
        val_high = arr[2]
        val_low = arr[3]
        val_close = arr[4]
        val_adjclose = arr[5]
        val_volume = arr[6]
        if not val_volume:
          val_volume = 0
        if val_time and val_low and val_low != 'null' and val_high and val_high != 'null' and val_open and val_open != 'null' and val_close and val_close != 'null' and val_volume:
          sql_select = "select ticker, time from tickers_data where ticker = " + str(ticker_id) + " and time = " + str(val_time)
          cur.execute(sql_select)
          cur_res = cur.fetchone()
          if cur_res and len(cur_res) > 0:
            print("data exists: " + str(ticker_id) + ", " + str(val_time))
            continue
          sql_delete = "DELETE FROM " + tickers_data_table + " where ticker = " + str(ticker_id) + " and time = " + str(val_time) + ";"
          cur.execute(sql_delete)
          sql_insert_ticker_data = "INSERT INTO " + tickers_data_table + "(ticker, time, open, high, low, close, adjclose, volume)" \
                                                                         " VALUES(" + str(ticker_id) + ", " + str(val_time) + ", " + str(val_open) + ", " \
                                   + str(val_high) + ", " + str(val_low) + ", " + str(val_close) + ", " \
                                   + str(val_adjclose) + ", " + str(val_volume) + ");"
          print(sql_insert_ticker_data)
          cur.execute(sql_insert_ticker_data)
          conn.commit()
    except psycopg2.DatabaseError as error:
      print(error)
      print("roll back.")
      conn.rollback()
    except Exception as error:
      print(error)
    finally:
      i = i + 1
except(Exception, psycopg2.DatabaseError) as error:
  print(error)
finally:
  if conn is not None:
    conn.close()
    print('Database connection closed.')
