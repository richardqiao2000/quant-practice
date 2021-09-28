select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
from cluster_2year a, (select ticker, max(distr_count) distr_count from cluster_2year group by ticker) b
where a.ticker = b.ticker and a.distr_count = b.distr_count;

/* 2 year cluster stock selection*/
select tk.name, lp.*, c.*, (c.distr_range_start - lp.price) / lp.price rate
from latest_price lp,
(select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
from cluster_2year a, (select ticker, max(distr_count) distr_count from cluster_2year group by ticker) b
where a.ticker = b.ticker and a.distr_count = b.distr_count) c, tickers tk
where lp.ticker = c.ticker
  and lp.price < c.distr_range_start
  and (c.distr_range_start - lp.price) / lp.price > 0.2
  and lp.ticker = tk.id
order by rate desc;

/* 1 year cluster stock selection*/
select tk.name, lp.*, c.*, (c.distr_range_start - lp.price) / lp.price rate
from latest_price lp,
(select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
from cluster_1year a, (select ticker, max(distr_count) distr_count from cluster_1year group by ticker) b
where a.ticker = b.ticker and a.distr_count = b.distr_count) c, tickers tk
where lp.ticker = c.ticker
  and lp.price < c.distr_range_start
  and (c.distr_range_start - lp.price) / lp.price > 0.2
  and lp.ticker = tk.id
order by rate desc;

/* 6 month cluster stock selection*/
select tk.name, lp.*, c.*, (c.distr_range_start - lp.price) / lp.price rate
from latest_price lp,
(select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
from cluster_6month a, (select ticker, max(distr_count) distr_count from cluster_6month group by ticker) b
where a.ticker = b.ticker and a.distr_count = b.distr_count) c, tickers tk
where lp.ticker = c.ticker
  and lp.price < c.distr_range_start
  and (c.distr_range_start - lp.price) / lp.price > 0.2
  and lp.ticker = tk.id
order by rate desc;

/* 3 month cluster stock selection*/
select tk.name, lp.*, c.*, (c.distr_range_start - lp.price) / lp.price rate
from latest_price lp,
(select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
from cluster_3month a, (select ticker, max(distr_count) distr_count from cluster_3month group by ticker) b
where a.ticker = b.ticker and a.distr_count = b.distr_count) c, tickers tk
where lp.ticker = c.ticker
  and lp.price < c.distr_range_start
  and (c.distr_range_start - lp.price) / lp.price > 0.2
  and lp.ticker = tk.id
order by rate desc;

/* 1 month cluster stock selection*/
select tk.name, lp.*, c.*, (c.distr_range_start - lp.price) / lp.price rate
from latest_price lp,
(select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
from cluster_1month a, (select ticker, max(distr_count) distr_count from cluster_1month group by ticker) b
where a.ticker = b.ticker and a.distr_count = b.distr_count) c, tickers tk
where lp.ticker = c.ticker
  and lp.price < c.distr_range_start
  and (c.distr_range_start - lp.price) / lp.price > 0.2
  and lp.ticker = tk.id
order by rate desc;

select count(*)
from latest_price lp,
(select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
from cluster_2year a, (select ticker, max(distr_count) distr_count from cluster_2year group by ticker) b
where a.ticker = b.ticker and a.distr_count = b.distr_count) c
where lp.ticker = c.ticker
  and lp.price < c.distr_range_start;
