/* raw data*/
select t.name, to_timestamp(td.time), td.*
from tickers_data td, tickers t
where t."name" = 'SNAP'
  and t.id = td.ticker
  and td.time >= extract(epoch from now()) - 3 * 30 * 24 * 3600;

/* cluster */
select tk.name, to_timestamp(lp.ts), lp.price, c.*, (c.distr_range_start - lp.price) / lp.price rate
from latest_price lp,
     (select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
      from cluster_3month a, (select ticker, max(distr_count) distr_count from cluster_3month group by ticker) b
      where a.ticker = b.ticker and a.distr_count = b.distr_count) c, tickers tk
where lp.ticker = c.ticker
  and lp.price < c.distr_range_start
  and (c.distr_range_start - lp.price) / lp.price > 0.2
  and lp.ticker = tk.id
order by rate desc;

/* trend */
select t.name, *
from trend tr, tickers t
where tr.ticker = t.id
  and tr.day30 is not null
order by tr.day30 desc;

/* 3 month cluster and 7 day trend */
select t.name, tr.day7, c_3month.*
from trend tr, tickers t, (
    select lp.ticker, lp.price, c.distr_index, c.distr_range_start, c.distr_range_end, c.distr_count, (c.distr_range_start - lp.price) / lp.price rate
    from latest_price lp,
         (select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
          from cluster_3month a, (select ticker, max(distr_count) distr_count from cluster_3month group by ticker) b
          where a.ticker = b.ticker and a.distr_count = b.distr_count) c, tickers tk
    where lp.ticker = c.ticker
      and lp.price < c.distr_range_start
      and lp.ticker = tk.id
) c_3month
where tr.ticker = t.id
  and tr.day7 is not null
  and tr.day7 > 0
  and c_3month.ticker = tr.ticker
order by rate desc

/* 6 month cluster and 7 day trend */
select t.name, tr.day7, c_3month.*
from trend tr, tickers t, (
    select lp.ticker, lp.price, c.distr_index, c.distr_range_start, c.distr_range_end, c.distr_count, (c.distr_range_start - lp.price) / lp.price rate
    from latest_price lp,
         (select a.ticker, a.distr_index, a.distr_range_start, a.distr_range_end, a.distr_count
          from cluster_6month a, (select ticker, max(distr_count) distr_count from cluster_6month group by ticker) b
          where a.ticker = b.ticker and a.distr_count = b.distr_count) c, tickers tk
    where lp.ticker = c.ticker
      and lp.price < c.distr_range_start
      and lp.ticker = tk.id
) c_3month
where tr.ticker = t.id
  and tr.day7 is not null
  and tr.day7 > 0
  and c_3month.ticker = tr.ticker
order by rate desc

