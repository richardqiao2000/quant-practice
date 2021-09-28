from datetime import datetime
import psycopg2
import pandas as pd
import sqlalchemy
import statistics


# Cluster range: 4 Month
# Inc_Index: > median
# Volume: no limit
# Target day: 2020-11-03
# 1) Set target day
target_day = "2021-02-01"
# 2) Set cluster range start day
cluster_range_start_day = "2020-12-01"
# 3) Set back test day
back_test = False
test_day = "2021-01-29"

# get all ticker name and id
try:
  sql = "select * from tickers order by name"
  engine = sqlalchemy.create_engine('postgresql://rqiao2:@localhost/stock_data')
  df_tickers = pd.read_sql(sql, con = engine)
except(Exception, psycopg2.DatabaseError) as error:
  print(error)

# Step 1: get target date's price
utc_time = datetime.strptime(target_day + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
target_day_epoch_time = round((utc_time - datetime(1970, 1, 1)).total_seconds())

try:
  engine = sqlalchemy.create_engine('postgresql://rqiao2:@localhost/stock_data')
  sql = "set time zone UTC; \n" \
        + "select ticker, to_timestamp(time)::timestamp without time zone target_time, open, high, low, adjclose, volume from tickers_data where time = " + str(target_day_epoch_time)
  #     print(sql)
  df_price_target_day = pd.read_sql(sql, con = engine)
except(Exception, psycopg2.DatabaseError) as error:
  print(error)

# Step 2: build price cluster in the range of [start_date, target_date]
# 3 month cluster
utc_time = datetime.strptime(cluster_range_start_day + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
time_analyze_start = round((utc_time - datetime(1970, 1, 1)).total_seconds())

sql = "select * from tickers_data where time >= " \
      + str(time_analyze_start) + " and time <= " + str(target_day_epoch_time)
try:
  engine = sqlalchemy.create_engine('postgresql://rqiao2:@localhost/stock_data')
  df_price_in_range = pd.read_sql(sql, con = engine)
except(Exception, psycopg2.DatabaseError) as error:
  print(error)
column_names = ["ticker", "distr_index", "distr_range_start", "distr_range_end", "distr_count"]
df_cluster = pd.DataFrame(columns = column_names)
for index, row in df_tickers.iterrows():
  ticker_id = row['id']
  list_open = df_price_in_range[lambda x: x['ticker'] == ticker_id]['open'].tolist()
  list_high = df_price_in_range[lambda x: x['ticker'] == ticker_id]['high'].tolist()
  list_low = df_price_in_range[lambda x: x['ticker'] == ticker_id]['low'].tolist()
  list_close = df_price_in_range[lambda x: x['ticker'] == ticker_id]['adjclose'].tolist()
  list_open.extend(list_high)
  list_open.extend(list_low)
  list_open.extend(list_close)
  list_prices = list_open
  if len(list_prices) == 0:
    continue
  val_min = min(list_prices)
  val_max = max(list_prices)
  piece = (val_max - val_min) / 10
  # set 10 ranges
  range_stats = []
  distr = {}
  for i in range(10):
    range_stats.append(val_min + i * piece)
    distr[i] = 0
  range_stats.append(val_max)
  # number distribution stats
  for num in list_prices:
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
  # add row list
  row_list = []
  for i in range(len(distr)):
    range_start = range_stats[i]
    range_end = range_stats[i + 1]
    count = distr[i]
    row_list.append({'ticker':ticker_id, 'distr_index':i, 'distr_range_start':range_start,
                     'distr_range_end':range_end, 'distr_count':int(count)})
  #     print(ticker_id)
  df = pd.DataFrame(row_list)
  df_cluster = df_cluster.append(pd.DataFrame(row_list))

df_cluster['tmp_num'] = pd.factorize(df_cluster['distr_count'], sort=True)[0]
df_cluster['rank'] = df_cluster.groupby('ticker')['tmp_num'].rank(ascending=False)
del df_cluster['tmp_num']

idx = df_cluster.groupby(['ticker'])['distr_count'].transform(max) == df_cluster['distr_count']
df_cluster_max_distr = df_cluster[idx]
idx2 = df_cluster_max_distr.groupby(['ticker'])['distr_index'].transform(min) == df_cluster_max_distr['distr_index']
df_cluster_max_distr = df_cluster_max_distr[idx2]

#df_cluster[lambda x: x['ticker'] == 9181]
# df_cluster
df_tc = df_price_target_day.join(df_cluster.set_index('ticker'), on='ticker')
df_rank = df_tc.query('adjclose >= distr_range_start and adjclose <= distr_range_end')
del df_rank['open']
del df_rank['high']
del df_rank['low']
del df_rank['volume']
del df_rank['distr_index']
del df_rank['distr_range_start']
del df_rank['distr_range_end']
del df_rank['distr_count']
del df_rank['target_time']
del df_rank['adjclose']

# Step 3: build trends
#  (tr.day1 + tr.day7 + tr.day14 + tr.day30 + tr.month3 + tr.month6 + tr.year1 + tr.year2 + tr.year3) inc_index
def build_trend_sql(target_date, look_back_dates):
  sql = "select tmp_trend.ticker, min(tmp_trend.trend_rate) rate " \
        + "from(" \
        + "select td.ticker, td.adjclose, ((lp.adjclose - td.adjclose) / td.adjclose) trend_rate " \
        + "from (select * from tickers_data where time = " + str(target_date) + ") lp, " \
        + " tickers_data td, tickers t " \
        + "where lp.ticker = td.ticker " \
        + "  and t.id = lp.ticker " \
        + "  and (td.time = lp.time - (" + str(look_back_dates) + ") * 24 * 3600 or td.time = lp.time - (" + str(look_back_dates) + " + 1) * 24 * 3600 or td.time = lp.time - (" + str(look_back_dates) + " + 2) * 24 * 3600 or td.time = lp.time - (" + str(look_back_dates) + " + 3) * 24 * 3600) " \
        + ") tmp_trend " \
        + "group by tmp_trend.ticker "
  #     print(sql)
  return sql
try:
  engine = sqlalchemy.create_engine('postgresql://rqiao2:@localhost/stock_data')
  df_trend_day1 = pd.read_sql(build_trend_sql(target_day_epoch_time, 1), con = engine)
  df_trend_day7 = pd.read_sql(build_trend_sql(target_day_epoch_time, 7), con = engine)
  df_trend_day14 = pd.read_sql(build_trend_sql(target_day_epoch_time, 14), con = engine)
  df_trend_day30 = pd.read_sql(build_trend_sql(target_day_epoch_time, 30), con = engine)
  df_trend_month3 = pd.read_sql(build_trend_sql(target_day_epoch_time, 3 * 30), con = engine)
  df_trend_month6 = pd.read_sql(build_trend_sql(target_day_epoch_time, 6 * 30), con = engine)
  df_trend_year1 = pd.read_sql(build_trend_sql(target_day_epoch_time, 365), con = engine)
  df_trend_year2 = pd.read_sql(build_trend_sql(target_day_epoch_time, 2 * 365), con = engine)
  df_trend_year3 = pd.read_sql(build_trend_sql(target_day_epoch_time, 3 * 365), con = engine)

  df_trend_tmp = df_trend_day1.join(df_trend_day7.set_index('ticker'), on='ticker', how='inner', rsuffix='_7')
  df_trend_tmp = df_trend_tmp.join(df_trend_day14.set_index('ticker'), on='ticker', how='inner', rsuffix='_14')
  df_trend_tmp = df_trend_tmp.join(df_trend_day30.set_index('ticker'), on='ticker', how='inner', rsuffix='_30')
  df_trend_tmp = df_trend_tmp.join(df_trend_month3.set_index('ticker'), on='ticker', how='inner', rsuffix='_m3')
  df_trend_tmp = df_trend_tmp.join(df_trend_month6.set_index('ticker'), on='ticker', how='inner', rsuffix='_m6')
  df_trend_tmp = df_trend_tmp.join(df_trend_year1.set_index('ticker'), on='ticker', how='inner', rsuffix='_y1')
  df_trend_tmp = df_trend_tmp.join(df_trend_year1.set_index('ticker'), on='ticker', how='inner', rsuffix='_y2')
  df_trend_tmp = df_trend_tmp.join(df_trend_year1.set_index('ticker'), on='ticker', how='inner', rsuffix='_y3')
  df_trend_all = df_trend_tmp
  df_inc_index = (df_trend_all['rate'] * 8 + df_trend_all['rate_7'] * 7 + df_trend_all['rate_14'] * 6 + df_trend_all['rate_30'] * 5 \
                  + df_trend_all['rate_m3'] * 4 + df_trend_all['rate_m6'] * 3 + df_trend_all['rate_y1'] * 2 + df_trend_all['rate_y2'] \
                  + df_trend_all['rate_y3']) / 37
  df_trend_all['inc_index'] = df_inc_index
except(Exception, psycopg2.DatabaseError) as error:
  print(error)

inc_index_median = statistics.median(df_trend_all['inc_index'])


# Step 4: build volume median
def build_volume_median_sql(target_date, look_back_dates):
  sql = "select td.ticker, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median_vol " + \
        "from tickers_data td " + \
        "where td.time <= " + str(target_date) + " " + \
        "    and td.time >= " + str(target_date) + " - " + str(look_back_dates) + " * 24 * 3600 " + \
        "group by td.ticker"
  #     print(sql)
  return sql
try:
  engine = sqlalchemy.create_engine('postgresql://rqiao2:@localhost/stock_data')
  df_volume_median_day7 = pd.read_sql(build_volume_median_sql(target_day_epoch_time, 7), con = engine)
  df_volume_median_day14 = pd.read_sql(build_volume_median_sql(target_day_epoch_time, 14), con = engine)
  df_volume_median_day30 = pd.read_sql(build_volume_median_sql(target_day_epoch_time, 30), con = engine)
  df_volume_median_month3 = pd.read_sql(build_volume_median_sql(target_day_epoch_time, 3 * 30), con = engine)
  df_volume_median_month6 = pd.read_sql(build_volume_median_sql(target_day_epoch_time, 6 * 30), con = engine)
  df_volume_median_year1 = pd.read_sql(build_volume_median_sql(target_day_epoch_time, 365), con = engine)
  df_volume_median_year2 = pd.read_sql(build_volume_median_sql(target_day_epoch_time, 2 * 365), con = engine)
  df_volume_median_year3 = pd.read_sql(build_volume_median_sql(target_day_epoch_time, 3 * 365), con = engine)
except(Exception, psycopg2.DatabaseError) as error:
  print(error)

# Step 5: cluster-trend-volume analysis
df_targetprice_cluster = df_price_target_day.join(df_cluster_max_distr.set_index('ticker'), on='ticker')
df_targetprice_cluster_trend = df_targetprice_cluster.join(df_trend_all.set_index('ticker'), on='ticker')
df_targetprice_cluster_trend_volume1year = df_targetprice_cluster_trend.join(df_volume_median_year1.set_index('ticker'), on='ticker')

# trend day7 > 0, volume 1 year median >= 1000000, trend inc_index >= 0
# target_price < high frequency price start
df_tmp = df_targetprice_cluster_trend_volume1year.join(df_rank.set_index('ticker'), on='ticker', rsuffix='_freq')

# get recommendation result
condition_inc_index = ' & rate > 0 & inc_index < ' + str(10 * inc_index_median) + ' & inc_index >= 0 & rank_freq >= 4'
df_tmp2 = df_tmp.query('adjclose < distr_range_start * 0.85' + condition_inc_index)
del df_tmp2['rate_y1']
del df_tmp2['rate_y2']
del df_tmp2['rate_y3']
del df_tmp2['rate_m6']
del df_tmp2['rate_m3']
del df_tmp2['rate_30']
del df_tmp2['distr_range_end']
df_result = df_tickers.join(df_tmp2.set_index('ticker'), on='id', how='inner')
df_result.sort_values('distr_count', ascending=False)

# Step 6: back test
if back_test:
  # Step 6.1 get test day data
  test_day = "2020-12-01"
  utc_time = datetime.strptime(test_day + "T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
  test_day_epoch_time = round((utc_time - datetime(1970, 1, 1)).total_seconds())
  try:
    engine = sqlalchemy.create_engine('postgresql://rqiao2:@localhost/stock_data')
    sql = "set time zone UTC; \n" \
          + "select ticker, to_timestamp(time)::timestamp without time zone test_time, adjclose, volume " \
          + "from tickers_data where time = " + str(test_day_epoch_time)
    df_price_test_day = pd.read_sql(sql, con = engine)
  except(Exception, psycopg2.DatabaseError) as error:
    print(error)


  # Step 6.2 back test
  # Set condition
  df_test_target = df_price_test_day.join(df_tmp2.set_index('ticker'), on='ticker', how='inner', lsuffix='_test', rsuffix='_res')
  df_final = df_tickers.join(df_test_target.set_index('ticker'), on='id', how='inner')

  total = len(df_final)
  increased = len(df_final.query('adjclose_test > adjclose_res'))
  increase_rate = "{:.2%}".format(increased / total)

print(df_final.sort_values('distr_count', ascending=False))
