update trend tr set day1 = a.rate
from
(
select tmp_trend.ticker, min(tmp_trend.trend_rate) rate
from(
select t.name, lp.*, to_timestamp(lp.ts) today, td.time, to_timestamp(td.time) to_compare, td.adjclose, ((lp.price - td.adjclose) / td.adjclose) trend_rate
from latest_price lp, tickers_data td, tickers t
where lp.ticker = td.ticker
  and t.id = lp.ticker
  and (td.time = lp.ts - (1) * 24 * 3600 or td.time = lp.ts - (1 + 1) * 24 * 3600 or td.time = lp.ts - (1 + 2) * 24 * 3600)
) tmp_trend
group by tmp_trend.ticker, tmp_trend.name
) a
where tr.ticker = a.ticker;

update trend tr set day7 = a.rate
from
(
select tmp_trend.ticker, min(tmp_trend.trend_rate) rate
from(
select t.name, lp.*, to_timestamp(lp.ts) today, td.time, to_timestamp(td.time) to_compare, td.adjclose, ((lp.price - td.adjclose) / td.adjclose) trend_rate
from latest_price lp, tickers_data td, tickers t
where lp.ticker = td.ticker
  and t.id = lp.ticker
  and (td.time = lp.ts - (7) * 24 * 3600 or td.time = lp.ts - (7 + 1) * 24 * 3600 or td.time = lp.ts - (7 + 2) * 24 * 3600)
) tmp_trend
group by tmp_trend.ticker, tmp_trend.name
) a
where tr.ticker = a.ticker;

update trend tr set day14 = a.rate
from
(
select tmp_trend.ticker, min(tmp_trend.trend_rate) rate
from(
select t.name, lp.*, to_timestamp(lp.ts) today, td.time, to_timestamp(td.time) to_compare, td.adjclose, ((lp.price - td.adjclose) / td.adjclose) trend_rate
from latest_price lp, tickers_data td, tickers t
where lp.ticker = td.ticker
  and t.id = lp.ticker
  and (td.time = lp.ts - (14) * 24 * 3600 or td.time = lp.ts - (14 + 1) * 24 * 3600 or td.time = lp.ts - (14 + 2) * 24 * 3600)
) tmp_trend
group by tmp_trend.ticker, tmp_trend.name
) a
where tr.ticker = a.ticker;

update trend tr set day30 = a.rate
from
(
select tmp_trend.ticker, min(tmp_trend.trend_rate) rate
from(
select t.name, lp.*, to_timestamp(lp.ts) today, td.time, to_timestamp(td.time) to_compare, td.adjclose, ((lp.price - td.adjclose) / td.adjclose) trend_rate
from latest_price lp, tickers_data td, tickers t
where lp.ticker = td.ticker
  and t.id = lp.ticker
  and (td.time = lp.ts - (30) * 24 * 3600 or td.time = lp.ts - (30 + 1) * 24 * 3600 or td.time = lp.ts - (30 + 2) * 24 * 3600)
) tmp_trend
group by tmp_trend.ticker, tmp_trend.name
) a
where tr.ticker = a.ticker;

update trend tr set month3 = a.rate
from
(
select tmp_trend.ticker, min(tmp_trend.trend_rate) rate
from(
select t.name, lp.*, to_timestamp(lp.ts) today, td.time, to_timestamp(td.time) to_compare, td.adjclose, ((lp.price - td.adjclose) / td.adjclose) trend_rate
from latest_price lp, tickers_data td, tickers t
where lp.ticker = td.ticker
  and t.id = lp.ticker
  and (td.time = lp.ts - (3 * 30) * 24 * 3600 or td.time = lp.ts - (3 * 30 + 1) * 24 * 3600 or td.time = lp.ts - (3 * 30 + 2) * 24 * 3600)
) tmp_trend
group by tmp_trend.ticker, tmp_trend.name
) a
where tr.ticker = a.ticker;

update trend tr set month6 = a.rate
from
(
select tmp_trend.ticker, min(tmp_trend.trend_rate) rate
from(
select t.name, lp.*, to_timestamp(lp.ts) today, td.time, to_timestamp(td.time) to_compare, td.adjclose, ((lp.price - td.adjclose) / td.adjclose) trend_rate
from latest_price lp, tickers_data td, tickers t
where lp.ticker = td.ticker
  and t.id = lp.ticker
  and (td.time = lp.ts - (6 * 30) * 24 * 3600 or td.time = lp.ts - (6 * 30 + 1) * 24 * 3600 or td.time = lp.ts - (6 * 30 + 2) * 24 * 3600)
) tmp_trend
group by tmp_trend.ticker, tmp_trend.name
) a
where tr.ticker = a.ticker;

update trend tr set year1 = a.rate
from
(
select tmp_trend.ticker, min(tmp_trend.trend_rate) rate
from(
select t.name, lp.*, to_timestamp(lp.ts) today, td.time, to_timestamp(td.time) to_compare, td.adjclose, ((lp.price - td.adjclose) / td.adjclose) trend_rate
from latest_price lp, tickers_data td, tickers t
where lp.ticker = td.ticker
  and t.id = lp.ticker
  and (td.time = lp.ts - (365) * 24 * 3600 or td.time = lp.ts - (365 + 1) * 24 * 3600 or td.time = lp.ts - (365 + 2) * 24 * 3600)
) tmp_trend
group by tmp_trend.ticker, tmp_trend.name
) a
where tr.ticker = a.ticker;

update trend tr set year2 = a.rate
from
(
select tmp_trend.ticker, min(tmp_trend.trend_rate) rate
from(
select t.name, lp.*, to_timestamp(lp.ts) today, td.time, to_timestamp(td.time) to_compare, td.adjclose, ((lp.price - td.adjclose) / td.adjclose) trend_rate
from latest_price lp, tickers_data td, tickers t
where lp.ticker = td.ticker
  and t.id = lp.ticker
  and (td.time = lp.ts - (2 * 365) * 24 * 3600 or td.time = lp.ts - (2 * 365 + 1) * 24 * 3600 or td.time = lp.ts - (2 * 365 + 2) * 24 * 3600)
) tmp_trend
group by tmp_trend.ticker, tmp_trend.name
) a
where tr.ticker = a.ticker;

update trend tr set year3 = a.rate
from
(
select tmp_trend.ticker, min(tmp_trend.trend_rate) rate
from(
select t.name, lp.*, to_timestamp(lp.ts) today, td.time, to_timestamp(td.time) to_compare, td.adjclose, ((lp.price - td.adjclose) / td.adjclose) trend_rate
from latest_price lp, tickers_data td, tickers t
where lp.ticker = td.ticker
  and t.id = lp.ticker
  and (td.time = lp.ts - (3 * 365) * 24 * 3600 or td.time = lp.ts - (3 * 365 + 1) * 24 * 3600 or td.time = lp.ts - (3 * 365 + 2) * 24 * 3600)
) tmp_trend
group by tmp_trend.ticker, tmp_trend.name
) a
where tr.ticker = a.ticker;


