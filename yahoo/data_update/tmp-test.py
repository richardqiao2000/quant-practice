import psycopg2

nasdaq_file_path = "/Users/rqiao2/code/py-practice/stock/companies/nasdaqlisted.txt"
other_file_path = "/Users/rqiao2/code/py-practice/stock/companies/otherlisted.txt"
file_path = nasdaq_file_path
tickers_table = "tickers"
conn = None
try:
  conn = psycopg2.connect("dbname=stock_data user=rqiao2 password=")
  cur = conn.cursor()
  with open(file_path) as fp:
    fp.readline()
    line = fp.readline()
    while line:
      arr = line.split("|")
      ticker = arr[0]
      # 3) save stock price into database
      try:
        sql_select_ticker = "select id from " + tickers_table + " where name = '" + ticker + "';"
        sql_insert_ticker = "INSERT INTO " + tickers_table + "(name) VALUES('" + ticker + "');"
        cur.execute(sql_select_ticker)
        cur_res = cur.fetchone()
        if not cur_res or len(cur_res) == 0:
          print(sql_insert_ticker)
        #           cur.execute(sql_insert_ticker, (ticker,))
        #           conn.commit()
        else:
          print(ticker)
      except psycopg2.DatabaseError as error:
        print(error)
        print("roll back.")
        conn.rollback()
      except Exception as error:
        print(error)
      line = fp.readline()
except(Exception, psycopg2.DatabaseError) as error:
  print(error)
finally:
  if conn is not None:
    conn.close()
    print('Database connection closed.')