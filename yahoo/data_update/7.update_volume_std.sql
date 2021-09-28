select td.ticker, td.volume
from latest_price lp, tickers_data td, tickers t2
where lp.ticker = td.ticker
  and td.time <= lp.ts
  and td.time >= lp.ts - 7 * 24 * 3600
  and td.ticker = t2.id
  and t2.name = 'PAICU'

update volume_std vs set day7 = b.rate
from (
         select ticker, std, median, (std / median) rate
         from(
                 select td.ticker, stddev(td.volume) std, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
                 from latest_price lp, tickers_data td, tickers t2
                 where lp.ticker = td.ticker
                   and td.time <= lp.ts
                   and td.time >= lp.ts - 7 * 24 * 3600
                   and td.ticker = t2.id
                 group by td.ticker
             ) a
         where std is not null
           and median is not null and median != 0
     ) b
where vs.ticker = b.ticker

update volume_std vs set day14 = b.rate
from (
         select ticker, std, median, (std / median) rate
         from(
                 select td.ticker, stddev(td.volume) std, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
                 from latest_price lp, tickers_data td, tickers t2
                 where lp.ticker = td.ticker
                   and td.time <= lp.ts
                   and td.time >= lp.ts - 14 * 24 * 3600
                   and td.ticker = t2.id
                 group by td.ticker
             ) a
         where std is not null
           and median is not null and median != 0
     ) b
where vs.ticker = b.ticker

update volume_std vs set day30 = b.rate
from (
         select ticker, std, median, (std / median) rate
         from(
                 select td.ticker, stddev(td.volume) std, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
                 from latest_price lp, tickers_data td, tickers t2
                 where lp.ticker = td.ticker
                   and td.time <= lp.ts
                   and td.time >= lp.ts - 30 * 24 * 3600
                   and td.ticker = t2.id
                 group by td.ticker
             ) a
         where std is not null
           and median is not null and median != 0
     ) b
where vs.ticker = b.ticker

update volume_std vs set month3 = b.rate
from (
         select ticker, std, median, (std / median) rate
         from(
                 select td.ticker, stddev(td.volume) std, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
                 from latest_price lp, tickers_data td, tickers t2
                 where lp.ticker = td.ticker
                   and td.time <= lp.ts
                   and td.time >= lp.ts - 3 * 30 * 24 * 3600
                   and td.ticker = t2.id
                 group by td.ticker
             ) a
         where std is not null
           and median is not null and median != 0
     ) b
where vs.ticker = b.ticker

update volume_std vs set month6 = b.rate
from (
         select ticker, std, median, (std / median) rate
         from(
                 select td.ticker, stddev(td.volume) std, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
                 from latest_price lp, tickers_data td, tickers t2
                 where lp.ticker = td.ticker
                   and td.time <= lp.ts
                   and td.time >= lp.ts - 6 * 30 * 24 * 3600
                   and td.ticker = t2.id
                 group by td.ticker
             ) a
         where std is not null
           and median is not null and median != 0
     ) b
where vs.ticker = b.ticker

update volume_std vs set year1 = b.rate
from (
         select ticker, std, median, (std / median) rate
         from(
                 select td.ticker, stddev(td.volume) std, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
                 from latest_price lp, tickers_data td, tickers t2
                 where lp.ticker = td.ticker
                   and td.time <= lp.ts
                   and td.time >= lp.ts - 365 * 24 * 3600
                   and td.ticker = t2.id
                 group by td.ticker
             ) a
         where std is not null
           and median is not null and median != 0
     ) b
where vs.ticker = b.ticker

update volume_std vs set year2 = b.rate
from (
         select ticker, std, median, (std / median) rate
         from(
                 select td.ticker, stddev(td.volume) std, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
                 from latest_price lp, tickers_data td, tickers t2
                 where lp.ticker = td.ticker
                   and td.time <= lp.ts
                   and td.time >= lp.ts - 2 * 365 * 24 * 3600
                   and td.ticker = t2.id
                 group by td.ticker
             ) a
         where std is not null
           and median is not null and median != 0
     ) b
where vs.ticker = b.ticker

update volume_std vs set year3 = b.rate
from (
         select ticker, std, median, (std / median) rate
         from(
                 select td.ticker, stddev(td.volume) std, percentile_disc(0.5) WITHIN GROUP (ORDER BY td.volume) median
                 from latest_price lp, tickers_data td, tickers t2
                 where lp.ticker = td.ticker
                   and td.time <= lp.ts
                   and td.time >= lp.ts - 3 * 365 * 24 * 3600
                   and td.ticker = t2.id
                 group by td.ticker
             ) a
         where std is not null
           and median is not null and median != 0
     ) b
where vs.ticker = b.ticker

