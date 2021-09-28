import psycopg2
from os import listdir
from os.path import isfile, join
import urllib.request
import ssl
import requests
from datetime import datetime
from datetime import date
import time

def store_tickers_latest_price(list_ticker_files, tickers_table, latest_price_table, time_start, time_end, last_ticker_file):
  conn = None
  attemp = 0
  try:
    conn = psycopg2.connect("dbname=stock_data user=rqiao2 password=")
    i = 0
    end_index = len(list_ticker_files)
    while last_ticker_file and list_ticker_files[i] != last_ticker_file:
      i = i + 1
    while i < end_index:
      ticker_file = list_ticker_files[i]
      ticker = ticker_file.replace(".csv", "")
      print(ticker)

      # 2) Get data from yahoo service
      list_daily_data = []
      try:
        url = "https://query1.finance.yahoo.com/v7/finance/download/" \
              + ticker + "?period1=" + str(time_start) + "&period2=" + str(time_end)\
              + "&interval=1d&events=history&includeAdjustedClose=true"
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
        attemp += 1
        if attemp == 2:
          attemp = 0
          i = i + 1
        else:
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
          time.sleep(10)
        continue

      # 3) save stock price into database
      try:
        sql_select_ticker = """select id from """ + tickers_table + """ where name = %s;"""
        sql_insert_ticker = "INSERT INTO " + tickers_table + "(name) VALUES(%s) RETURNING id;"
        cur = conn.cursor()
        cur.execute(sql_select_ticker, (ticker,))
        cur_res = cur.fetchone()
        if cur_res and len(cur_res) > 0:
          ticker_id = cur_res[0]
        else:
          cur.execute(sql_insert_ticker, (ticker,))
          conn.commit()
          cur.execute(sql_select_ticker, (ticker,))
          cur_res = cur.fetchone()
          ticker_id = cur_res[0]
        last_epoch_time = -1
        for i_daily in range(1, len(list_daily_data)):
          line = list_daily_data[i_daily]
          arr = line.split(",")
          utc_time = datetime.strptime(arr[0] + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
          epoch_time = round((utc_time - datetime(1970, 1, 1)).total_seconds())
          if last_epoch_time == epoch_time:
            continue
          val_adjclose = arr[5]
          if val_adjclose:
            sql_select = """select ticker from """ + latest_price_table + """ where ticker = %s;"""
            cur = conn.cursor()
            cur.execute(sql_select, (ticker_id,))
            cur_res = cur.fetchone()
            if cur_res and len(cur_res) > 0:
              ticker_id = cur_res[0]
              sql_update_ticker_data = "UPDATE " + latest_price_table\
                                       + " SET ts = " + str(epoch_time) + ", price = " + str(val_adjclose) + " where ticker = "\
                                       + str(ticker_id) + ";"
              print(sql_update_ticker_data)
              cur.execute(sql_update_ticker_data)
              conn.commit()
            else:
              sql_insert_ticker_data = "INSERT INTO " + latest_price_table + "(ticker, ts, price)" \
                                       " VALUES(" + str(ticker_id) + ", " + str(epoch_time) + ", " + str(val_adjclose) + ");"
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


# 2) Load Tickers
file_path = "../../stock_data/"
ticker_files = sorted([f for f in listdir(file_path) if isfile(join(file_path, f))])

# 3) retrieve data and save it to database
today = date.today().strftime("%Y-%m-%d")
utc_time = datetime.strptime(today + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
today_epoch_time = round((utc_time - datetime(1970, 1, 1)).total_seconds())
store_tickers_latest_price(ticker_files, "tickers", "latest_price", today_epoch_time + 24 * 3600, today_epoch_time + 24 * 3600, None)
