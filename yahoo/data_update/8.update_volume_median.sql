select td.ticker, td.volume
from latest_price lp, tickers_data td, tickers t2
where lp.ticker = td.ticker
  and td.time <= lp.ts
  and td.time >= lp.ts - 30 * 24 * 3600
  and td.ticker = t2.id
  and t2.name = 'HYTR'

select * from volume_median vs2;

update volume_median vs set day7 = a.median
from(
        select td.ticker, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
        from latest_price lp, tickers_data td, tickers t2
        where lp.ticker = td.ticker
          and td.time <= lp.ts
          and td.time >= lp.ts - 7 * 24 * 3600
          and td.ticker = t2.id
        group by td.ticker
    ) a
where vs.ticker = a.ticker;

update volume_median vs set day14 = a.median
from(
        select td.ticker, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
        from latest_price lp, tickers_data td, tickers t2
        where lp.ticker = td.ticker
          and td.time <= lp.ts
          and td.time >= lp.ts - 14 * 24 * 3600
          and td.ticker = t2.id
        group by td.ticker
    ) a
where vs.ticker = a.ticker;

update volume_median vs set day30 = a.median
from(
        select td.ticker, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
        from latest_price lp, tickers_data td, tickers t2
        where lp.ticker = td.ticker
          and td.time <= lp.ts
          and td.time >= lp.ts - 30 * 24 * 3600
          and td.ticker = t2.id
        group by td.ticker
    ) a
where vs.ticker = a.ticker;

update volume_median vs set month3 = a.median
from(
        select td.ticker, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
        from latest_price lp, tickers_data td, tickers t2
        where lp.ticker = td.ticker
          and td.time <= lp.ts
          and td.time >= lp.ts - 3 * 30 * 24 * 3600
          and td.ticker = t2.id
        group by td.ticker
    ) a
where vs.ticker = a.ticker;

update volume_median vs set month6 = a.median
from(
        select td.ticker, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
        from latest_price lp, tickers_data td, tickers t2
        where lp.ticker = td.ticker
          and td.time <= lp.ts
          and td.time >= lp.ts - 6 * 30 * 24 * 3600
          and td.ticker = t2.id
        group by td.ticker
    ) a
where vs.ticker = a.ticker;

update volume_median vs set year1 = a.median
from(
        select td.ticker, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
        from latest_price lp, tickers_data td, tickers t2
        where lp.ticker = td.ticker
          and td.time <= lp.ts
          and td.time >= lp.ts - 365 * 24 * 3600
          and td.ticker = t2.id
        group by td.ticker
    ) a
where vs.ticker = a.ticker;

update volume_median vs set year2 = a.median
from(
        select td.ticker, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
        from latest_price lp, tickers_data td, tickers t2
        where lp.ticker = td.ticker
          and td.time <= lp.ts
          and td.time >= lp.ts - 2 * 365 * 24 * 3600
          and td.ticker = t2.id
        group by td.ticker
    ) a
where vs.ticker = a.ticker;

update volume_median vs set year3 = a.median
from(
        select td.ticker, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
        from latest_price lp, tickers_data td, tickers t2
        where lp.ticker = td.ticker
          and td.time <= lp.ts
          and td.time >= lp.ts - 3 * 365 * 24 * 3600
          and td.ticker = t2.id
        group by td.ticker
    ) a
where vs.ticker = a.ticker;

