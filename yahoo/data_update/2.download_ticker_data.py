import psycopg2
import requests
import urllib.request
import ssl
import datetime

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


def download_tickers_data(list_tickers, tickers_table, tickers_data_table, time_start, time_end, last_ticker):
  conn = None
  try:
    conn = psycopg2.connect("dbname=postgres user=rqiao2 password=")
    i = 0
    end = len(list_tickers)
    while last_ticker and list_tickers[i] != last_ticker:
      i = i + 1
    while i < end:
      ticker = list_tickers[i]
      try:
        url = "https://query1.finance.yahoo.com/v7/finance/download/" \
              + ticker + "?period1=" + str(time_start) + "&period2=" + str(time_end) + "&interval=1d&events=history&includeAdjustedClose=true"
        # request and download file
        ssl._create_default_https_context = ssl._create_unverified_context
        response = urllib.request.urlopen(url)
        data = response.read()      # a `bytes` object
        text = data.decode('utf-8')
        path = "../../stock_data/"
        with open(path + ticker + ".csv", mode='w') as file_writer:
          file_writer.write(text)
      except requests.exceptions.ProxyError as error:
        print(error)
        continue
      except Exception as error:
        print(error)
        continue
      finally:
        i = i + 1
      print(ticker)
  except(Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')

def getEpochTime(dt):
  utc_time = datetime.strptime(dt + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
  return round((utc_time - datetime(1970, 1, 1)).total_seconds())


# 2) Load Tickers
file_nasdaq = "../../stock_data/tickers/NASDAQ.txt"
file_nyse = "../../stock_data/tickers/NYSE.txt"
file_amex = "../../stock_data/tickers/AMEX.txt"
list_nasdaq = build_ticker_list(file_nasdaq)
list_nyse = build_ticker_list(file_nyse)
list_amex = build_ticker_list(file_amex)

# 3) retrieve data and save it to database
start = getEpochTime("2015-01-01")
end = getEpochTime("2021-02-04")

download_tickers_data(list_nasdaq, "tickers_nasdaq", "tickers_data_nasdaq", start, end, None)
download_tickers_data(list_nyse, "tickers_nyse", "tickers_data_nyse", start, end, None)
download_tickers_data(list_amex, "tickers_amex", "tickers_data_amex", start, end, None)
