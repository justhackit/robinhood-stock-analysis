CREATE TABLE robinhood_raw_transactions (
    activity_date DATE NOT NULL,
    process_date DATE,
    settle_date DATE,
    instrument VARCHAR,
    description VARCHAR,
    trans_code VARCHAR,
    quantity NUMERIC,
    price numeric,
    amount NUMERIC,
    source VARCHAR,
    --valid values are 'Manual' or 'Report'
    PRIMARY KEY (
        activity_date,
        instrument,
        description,
        trans_code,
        quantity,
        price,
        amount
    )
);


--drop table robinhood_raw_transactions ;
--Add a transaction manually
--Add a stock buy
insert into robinhood_raw_transactions (
        activity_date,
        process_date,
        settle_date,
        instrument,
        description,
        trans_code,
        quantity,
        price,
        amount,
        source
    )
values(
        '2024-05-22',
        '2024-05-22',
        '2024-05-22',
        'AFRM',
        'Affirm|CUSIP: 00827B106',
        'Buy',
        '10',
        29.97,
        299.70,
        'Manual'
    )
    
    
 ---_______________VIEWS START____________---
    --cleaning/aggregating raw transactions
--drop view stocks.robinhood_transformed_transactions__vw;
CREATE OR replace VIEW stocks.robinhood_transformed_transactions__vw
AS
  WITH base_daily_aggs
       AS (SELECT activity_date,
                  instrument,
                  description,
                  trans_code,
                  SUM(quantity) AS quantity,
                  Avg(price)    AS price,
                  SUM(amount)   AS amount
           FROM   stocks.robinhood_raw_transactions rrt
           GROUP  BY 1,
                     2,
                     3,
                     4
           ORDER  BY 1),
       trade_type
       AS (SELECT *,
                  CASE
                    WHEN trans_code IN ( 'Buy', 'Sell' ) THEN 'stocks'
                    WHEN trans_code IN ( 'STO', 'STC', 'BTO', 'BTC' ) THEN
                    'options'
                  END AS trade_type
           FROM   base_daily_aggs)
  SELECT *
  FROM   trade_type;

 
----------------------------------VIEW :Running totals for stocks----------------------- 
CREATE OR replace VIEW stocks.robinhood_stocks_cumulative__vw
AS
  WITH adjusted_numbers
       AS (SELECT instrument,
                  trans_code,
                  activity_date,
                  quantity,
                  CASE
                    WHEN Lower(trans_code) = 'sell' THEN quantity * -1
                    ELSE quantity
                  END AS quantity_adj,
                  amount,
                  price
           FROM   stocks.robinhood_transformed_transactions__vw rrt
          --where instrument='AFRM'
          ),
       ordered_transactions
       AS (SELECT instrument,
                  activity_date,
                  quantity_adj,
                  trans_code,
                  amount,
                  SUM(quantity_adj)
                    over (
                      PARTITION BY instrument
                      ORDER BY activity_date ) AS cumulative_total_quantity,
                  SUM(amount)
                    over (
                      PARTITION BY instrument
                      ORDER BY activity_date ) AS cumulative_total_investment
           FROM   adjusted_numbers
           WHERE  1 = 1
                  AND trans_code IN( 'Buy', 'Sell' ))
  SELECT instrument,
         activity_date,
         cumulative_total_quantity,
         cumulative_total_investment --,cumulative_average_price
  FROM   ordered_transactions
  GROUP  BY 1,
            2,
            3,
            4
  ORDER  BY activity_date; 

select *
from stocks.robinhood_stocks_cumulative__vw
where instrument = 'AMZN';



--Running totals for Options
select * from stocks.robinhood_transformed_transactions__vw where trade_type='options' and  instrument ='AFRM' order by activity_date desc;
select sum(amount) from stocks.robinhood_transformed_transactions__vw where trade_type='options' and  instrument ='AFRM';
select sum(amount),sum(quantity) from stocks.robinhood_transformed_transactions__vw where trade_type='stocks' and  instrument ='AFRM';
select sum(amount)  from stocks.robinhood_raw_transactions where trans_code not in ('Buy','Sell','OEXP','SLIP') and  instrument ='AFRM'

-------------------END : robinhood_stocks_cumulative__vw---------------------------



----------------------------------FUNCTION : GET AGGREAGTES SINCE A DATE. THIS RETURNS AGGS FOR STOCKS AND OPTIONS SEPERATELY-----------------------
--DROP FUNCTION stocks.get_aggregates_since__F;
CREATE OR REPLACE FUNCTION stocks.get_aggregates_since__F(
    p_date DATE,
    p_symbol VARCHAR
)
RETURNS TABLE (
    instrument VARCHAR,
    stock_net_value numeric,
    stocks_net_quantity numeric,
    options_net_value numeric
) AS $$
BEGIN
    RETURN QUERY
	    WITH options_net AS (
	        SELECT r.instrument,
	               SUM(r.amount) AS options_net_value
	        FROM stocks.robinhood_transformed_transactions__vw r
	        WHERE r.trade_type = 'options' 
	          AND r.activity_date >= p_date
	          AND r.instrument = p_symbol
	        GROUP BY r.instrument
	    )
,
    stocks_net_step0 AS (
        SELECT s.instrument,
               CASE
                 WHEN LOWER(s.trans_code) = 'sell' THEN s.quantity * -1
                 ELSE s.quantity
               END AS quantity,
               s.amount
        FROM stocks.robinhood_transformed_transactions__vw s
        WHERE s.trade_type = 'stocks'
        	          AND s.activity_date >= p_date
	          AND s.instrument = p_symbol
    ) ,
    stocks_net AS (
        SELECT ss.instrument,
               SUM(ss.amount) AS stock_net_value,
               SUM(ss.quantity) AS net_quantity
        FROM stocks_net_step0 ss
        GROUP BY ss.instrument
    )
     SELECT s.instrument,
           s.stock_net_value,
           s.net_quantity as stocks_net_quantity,
--           ((s.stock_net_value + o.options_net_value) / s.net_quantity) AS avg_price
           o.options_net_value
    FROM stocks_net s
    JOIN options_net o ON s.instrument = o.instrument;
END;
$$ LANGUAGE plpgsql;

select * from stocks.get_aggregates_since__F('2000-01-01','NKE')
---------------------------------------------



---_______________VIEWS END____________---




--delete from robinhood_raw_transactions;
select count(*)
from robinhood_raw_transactions
limit 100;
select *
from robinhood_raw_transactions
where instrument = 'AFRM'
order by activity_date asc;
select description,
    trans_code
from robinhood_raw_transactions
where instrument = 'AFRM'
group by 1,
    2;
--Your total cash in the game
select sum(amount)
from robinhood_raw_transactions
where instrument = ''
    and description like '%ACH%';
select sum(amount)
from robinhood_raw_transactions
where instrument = 'AFRM'
    and trans_code != 'OEXP'
select sum(amount)
from robinhood_raw_transactions
where instrument = 'AMZN'
    and trans_code != 'OEXP'
    and activity_date >= '2022-08-19'
select *
from robinhood_raw_transactions rrt
where instrument = 'AMZN'
    and amount IS DISTINCT
FROM 'NaN';