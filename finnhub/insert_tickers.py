import finnhub
import time
import psycopg2


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


def insert_tickers(list_member, tickers_table):
  conn = None
  try:
    conn = psycopg2.connect("dbname=postgres user=rqiao2 password=")
    for ticker in list_member:
      sql_insert_ticker = "INSERT INTO " + tickers_table + "(name) VALUES(%s) RETURNING id;"
      cur = conn.cursor()
      cur.execute(sql_insert_ticker, (ticker,))
    conn.commit()
  except(Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')


# 1) Setup client
finnhub_client = finnhub.Client(api_key="c0938h748v6tm13roud0")
# 2) Load Tickers
file_nasdaq = "../stock_data/tickers/NASDAQ.txt"
file_nyse = "../stock_data/tickers/NYSE.txt"
file_amex = "../stock_data/tickers/AMEX.txt"
list_nasdaq = build_ticker_list(file_nasdaq)
list_nyse = build_ticker_list(file_nyse)
list_amex = build_ticker_list(file_amex)

# 4) Save to database
insert_tickers(list_nasdaq, "tickers_nasdaq")
insert_tickers(list_nyse, "tickers_nyse")
insert_tickers(list_amex, "tickers_amex")

