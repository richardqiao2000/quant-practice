from os import listdir
from os.path import isfile, join
from datetime import datetime
from datetime import date
import psycopg2

DATA_FILE_PATH = "../../stock_data/"


def create_cluster(cluster_table, time_analyze_start, file_history_check, ticker_to_start):
  conn = None
  try:
    conn = psycopg2.connect("dbname=stock_data user=rqiao2 password=")
    for file in ticker_files:
      if file_history_check and file != ticker_to_start:
        continue
      file_history_check = False
      ticker = file.replace(".csv", "")
      file_path = DATA_FILE_PATH + file
      # read file line by line to get close price into a list
      list_close = []
      with open(file_path) as fp:
        fp.readline()
        line = fp.readline()
        while line:
          arr = line.split(",")
          utc_time = datetime.strptime(arr[0] + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
          epoch_time = round((utc_time - datetime(1970, 1, 1)).total_seconds())
          if epoch_time < time_analyze_start:
            line = fp.readline()
            continue
          if arr[-2] == 'null':
            line = fp.readline()
            continue
          list_close.append(float(arr[-2]))
          line = fp.readline()

      # get min and max close price
      if len(list_close) == 0:
        continue
      val_min = min(list_close)
      val_max = max(list_close)
      piece = (val_max - val_min) / 10
      # set 10 ranges
      range_stats = []
      distr = {}
      for i in range(10):
        range_stats.append(val_min + i * piece)
        distr[i] = 0
      range_stats.append(val_max)
      # number distribution stats
      for num in list_close:
        if range_stats[0] <= num < range_stats[1]:
          distr[0] += 1
        elif range_stats[1] <= num < range_stats[2]:
          distr[1] += 1
        elif range_stats[2] <= num < range_stats[3]:
          distr[2] += 1
        elif range_stats[3] <= num < range_stats[4]:
          distr[3] += 1
        elif range_stats[4] <= num < range_stats[5]:
          distr[4] += 1
        elif range_stats[5] <= num < range_stats[6]:
          distr[5] += 1
        elif range_stats[6] <= num < range_stats[7]:
          distr[6] += 1
        elif range_stats[7] <= num < range_stats[8]:
          distr[7] += 1
        elif range_stats[8] <= num < range_stats[9]:
          distr[8] += 1
        elif range_stats[9] <= num <= val_max:
          distr[9] += 1

      try:
        # get ticker id
        sql_select_ticker = """select id from tickers where name = %s;"""
        sql_insert_ticker = "INSERT INTO tickers (name) VALUES(%s) RETURNING id;"
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

        # save into database
        for i in range(len(distr)):
          range_start = range_stats[i]
          range_end = range_stats[i + 1]
          count = distr[i]
          sql_insert = """insert into """ + cluster_table + """ values(""" + str(ticker_id) + """, """ \
                       + str(i) + """, """ + str(range_start) + """, """ \
                       + str(range_end) + """, """ + str(count) + """);"""
          print(sql_insert)
          cur.execute(sql_insert)
          conn.commit()
      except psycopg2.DatabaseError as error:
        print(error)
        print("roll back.")
        conn.rollback()
      except Exception as error:
        print(error)

  except psycopg2.DatabaseError as error:
    print(error)
    print("roll back.")
    conn.rollback()
  except(Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')


# Load Tickers
ticker_files = sorted([f for f in listdir(DATA_FILE_PATH) if isfile(join(DATA_FILE_PATH, f))])
target_day = date.today().strftime("%Y-%m-%d")
utc_time = datetime.strptime(target_day + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
today_epoch_time = round((utc_time - datetime(1970, 1, 1)).total_seconds())

time_analyze_start_1month = today_epoch_time - 30 * 24 * 3600
time_analyze_start_3month = today_epoch_time - 3 * 30 * 24 * 3600
time_analyze_start_6month = today_epoch_time - 6 * 30 * 24 * 3600
time_analyze_start_1year = today_epoch_time - 365 * 24 * 3600
time_analyze_start_2year = today_epoch_time - 2 * 365 * 24 * 3600
time_analyze_start_3year = today_epoch_time - 3 * 365 * 24 * 3600
time_analyze_start_5year = today_epoch_time - 5 * 365 * 24 * 3600

# Get cluster
create_cluster("cluster_1month", time_analyze_start_1month, False, None)
# create_cluster("cluster_3month", time_analyze_start_3month, False, None)
# create_cluster("cluster_6month", time_analyze_start_6month, False, None)
# create_cluster("cluster_1year", time_analyze_start_1year, False, None)
# create_cluster("cluster_2year", time_analyze_start_2year, False, None)
# create_cluster("cluster_3year", time_analyze_start_3year, False, None)
# create_cluster("cluster_5year", time_analyze_start_5year, False, None)

# rank the distribution
# check if current price lower than high hits
# analyze recent 2 week trend
# analyze all market current trend

