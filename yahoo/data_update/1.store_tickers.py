import psycopg2


def get_list(file_path):
  list_company = []
  with open(file_path) as fp:
    fp.readline()
    line = fp.readline()
    while line:
      arr = line.split("|")
      ticker = arr[0]
      if len(ticker) <= 8:
        list_company.append(ticker)
      line = fp.readline()
  return list_company


def store_tickers(list_company, tickers_table):
  conn = None
  try:
    conn = psycopg2.connect("dbname=stock_data user=rqiao2 password=")
    cur = conn.cursor()
    for ticker in list_company:
      try:
        sql_select_ticker = "select id from " + tickers_table + " where name = '" + ticker + "';"
        sql_insert_ticker = "INSERT INTO " + tickers_table + "(name) VALUES('" + ticker + "');"
        cur.execute(sql_select_ticker)
        cur_res = cur.fetchone()
        if not cur_res or len(cur_res) == 0:
          print(sql_insert_ticker)
          cur.execute(sql_insert_ticker, (ticker,))
          conn.commit()
        else:
          print(ticker)
      except psycopg2.DatabaseError as error:
        print(error)
        print("roll back.")
        conn.rollback()
      except Exception as error:
        print(error)
  except(Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')


# 1) download the latest company list from http://ftp.nasdaqtrader.com/Trader.aspx?id=symbollookup
#    You'll notice two files: nasdaqlisted.txt and otherlisted.txt. These two files will give you
#    the entire list of tradeable symbols
#    Put the files into folder ../../stock_data/
nasdaq_file_path = "/Users/rqiao2/code/py-practice/stock/companies/nasdaqlisted.txt"
other_file_path = "/Users/rqiao2/code/py-practice/stock/companies/otherlisted.txt"

# 2) retrieve data and save it to database
list_nasdaq = get_list(nasdaq_file_path)
list_others = get_list(other_file_path)

# print(len(list_nasdaq))
store_tickers(list_nasdaq, "tickers")
store_tickers(list_nasdaq, "tickers")
