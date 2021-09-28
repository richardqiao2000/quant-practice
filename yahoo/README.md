* 区间聚类
  * 三个月区间聚类
  * 半年区间聚类
  * 一年区间聚类
  * 两年区间聚类
  * 三年区间聚类
  * 五年区间聚类
* 风险分析
* 选股分析
  * 买股推荐
  * 卖股推荐
* 所有股票走向分析
* 当日结束分析
  * 更新当日数据
    * store_ticker_data_daily.py
    * update_latest_price.py
  * 滚动更新分析
    * 聚类: analyze_ticker_price_cluster.py
    * 趋势: analyze_ticker_price_trend.py
  *  
* YAHOO API
  * Explain: https://stackoverflow.com/questions/44030983/yahoo-finance-url-not-working
* Operations
  * Start postgresql service ```brew services start postgresql```
  * Stop postgresql service ```brew services stop postgresql```
  * start notebook ```jupyter notebook```