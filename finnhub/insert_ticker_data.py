import finnhub
import time
import psycopg2
import requests

# 60 API calls/minute
API_LIMIT_CALL = 59  # make a smaller call amount for safety
API_LIMIT_TIME = 60  # seconds


def build_ticker_list(path):
  list_tickers = []
  with open(path) as fp:
    fp.readline()
    line = fp.readline()
    while line:
      arr = line.split("\t")
      list_tickers.append(arr[0])
      line = fp.readline()
  return list_tickers


def insert_tickers_data(list_tickers, tickers_table, tickers_data_table, task_time, last_ticker):
  conn = None
  api_queue = []
  try:
    conn = psycopg2.connect("dbname=postgres user=rqiao2 password=")
    start = 0
    end = len(list_tickers)
    # start = 500
    # end = 501
    i = start
    while last_ticker and list_tickers[i] != last_ticker:
      i = i + 1
    while i < end:
      # 1) control app limit
      ticker = list_tickers[i]
      unix_sec = round(time.time())
      while len(api_queue) > 0 and unix_sec - api_queue[0] <= len(api_queue):
        time.sleep(3)
        unix_sec = round(time.time())
      while len(api_queue) > 0 and unix_sec - api_queue[0] > API_LIMIT_TIME:
        api_queue.pop(0)
      api_queue.append(unix_sec)
      print("last " + str(unix_sec - api_queue[0]) + " seconds, " + str(len(api_queue)) + " calls.")
      # 2) get stock price from web service
      try:
        res = finnhub_client.stock_candles(ticker, 'D', task_time - 12 * 3600, task_time)
      except finnhub.exceptions.FinnhubAPIException:
        print("API limit reached. Sleep 30 seconds and retry...")
        time.sleep(30)
        continue
      except requests.exceptions.ProxyError as error:
        print(error)
        continue
      except Exception as error:
        print(error)
        continue

      print(ticker)
      print(res)
      if res['s'] == "no_data":
        i = i + 1
        continue

      # 3) save stock price into database
      try:
        sql_select_ticker = "select id from " + tickers_table + " where name = %s;"
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
        val_time = res['t'][0]
        val_low = res['l'][0]
        val_high = res['h'][0]
        val_open = res['o'][0]
        val_close = res['c'][0]
        val_volume = res['v'][0]
        if not val_volume:
          val_volume = -1
        if val_time and val_low and val_high and val_open and val_close and val_volume:
          sql_insert_ticker_data = "INSERT INTO " + tickers_data_table + "(ticker, time, low, high, open, close, volume) " \
                                   "VALUES('" + str(ticker_id) + "', '" + str(val_time) + "', '" + str(val_low) + "', '" \
                                   + str(val_high) + "', '" + str(val_open) + "', '" + str(val_close) + "', '" + str(val_volume) + "');"
          print(sql_insert_ticker_data)
          cur.execute(sql_insert_ticker_data)
          conn.commit()
      except psycopg2.DatabaseError as error:
        print(error)
        print("roll back.")
        conn.rollback()
      finally:
        i = i + 1

  except(Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')


def execute_task(type):
  # get task from table tasks_$type
  task_table = "tasks_" + type
  # query tasks
  sql = "select time, last_ticker from " + task_table + " where status != 0;"
  task_time = 1611874800
  last_ticker = ""
  #insert_tickers_data(list_nasdaq, "tickers_" + type, "tickers_data_" + type, task_time, last_ticker)


# 1) Setup client
finnhub_client = finnhub.Client(api_key="c0938h748v6tm13roud0")
# 2) Load Tickers
file_nasdaq = "../stock_data/tickers/NASDAQ.txt"
file_nyse = "../stock_data/tickers/NYSE.txt"
file_amex = "../stock_data/tickers/AMEX.txt"
list_nasdaq = build_ticker_list(file_nasdaq)
list_nyse = build_ticker_list(file_nyse)
list_amex = build_ticker_list(file_amex)

# 3) retrieve data and save it to database
insert_tickers_data(list_nasdaq, "tickers_nasdaq", "tickers_data_nasdaq", 1611874800, None)
insert_tickers_data(list_nyse, "tickers_nyse", "tickers_data_nyse", 1611874800, None)
insert_tickers_data(list_amex, "tickers_amex", "tickers_data_amex", 1611874800, None)

# 5) Recommend tickers
#
