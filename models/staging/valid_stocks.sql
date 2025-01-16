with monthly_data as (
    select
        symbol,
        date_trunc('month', trade_date) as month,
        count(distinct trade_date) as trading_days
    from {{ ref('stocks_data') }}
    group by symbol, month
),

consistent_stocks as (
    select
        symbol
    from monthly_data
    group by symbol
    having count(distinct month) = 72  -- 72 tháng từ 1/2018 đến 12/2023
)

select
    symbol
from consistent_stocks