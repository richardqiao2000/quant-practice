import psycopg2
from datetime import datetime
from os import listdir
from os.path import isfile, join


def store_tickers_data(list_ticker_files, tickers_table, tickers_data_table, last_ticker_file):
  conn = None
  try:
    conn = psycopg2.connect("dbname=stock_data user=rqiao2 password=")
    i = 0
    end = len(list_ticker_files)
    while last_ticker_file and list_ticker_files[i] != last_ticker_file:
      i = i + 1
    while i < end:
      ticker_file = list_ticker_files[i]
      ticker = ticker_file.replace(".csv", "")
      print(ticker)
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

        ticker_file = "../stock_data/" + ticker_file
        with open(ticker_file) as fp:
          fp.readline()
          line = fp.readline()
          while line:
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
            if val_time and val_low and val_high and val_open and val_close and val_volume:
              sql_delete = "DELETE FROM " + tickers_data_table + " where ticker = " + str(ticker_id) + " and time = " + str(val_time) + ";"
              cur.execute(sql_delete)
              sql_insert_ticker_data = "INSERT INTO " + tickers_data_table + "(ticker, time, open, high, low, close, adjclose, volume)" \
                                       " VALUES(" + str(ticker_id) + ", " + str(val_time) + ", " + str(val_low) + ", "\
                                       + str(val_high) + ", " + str(val_open) + ", " + str(val_close) + ", "\
                                       + str(val_adjclose) + ", " + str(val_volume) + ");"
              print(sql_insert_ticker_data)
              cur.execute(sql_insert_ticker_data)
              conn.commit()
              line = fp.readline()
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
file_path = "/Users/rqiao2/code/py-practice/stock/stock_data/"
ticker_files = sorted([f for f in listdir(file_path) if isfile(join(file_path, f))])

# 3) retrieve data and save it to database
start = 1420153200  # 2015-01-01
end = 1611874800  # 2021-01-28
store_tickers_data(ticker_files, "tickers", "tickers_data", None)

# 5) Recommend tickers
#
