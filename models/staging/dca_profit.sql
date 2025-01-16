with first_trade_each_month as (
    select
        symbol,
        date_trunc('month', trade_date) as month,
        min(trade_date) as first_trade_date
    from {{ ref('stocks_data') }}
    where symbol in (select symbol from {{ ref('valid_stocks') }})
    group by symbol, month
),

dca_data as (
    select
        s.symbol,
        f.month,
        s.open as open_price,
        100 as shares_purchased, -- Số lượng cổ phiếu mua mỗi tháng
        100 * s.open as cost
    from {{ ref('stocks_data') }} s
    join first_trade_each_month f
    on s.symbol = f.symbol and s.trade_date = f.first_trade_date
),

total_costs as (
    select
        symbol,
        sum(cost) as total_invested,
        sum(shares_purchased) as total_shares_purchased
    from dca_data
    group by symbol
),

last_trade_per_symbol as (
    select
        symbol,
        max(trade_date) as last_trade_date
    from {{ ref('stocks_data') }}
    where trade_date <= '2023-12-31'
    group by symbol
),

current_values as (
    select
        s.symbol,
        t.total_invested,
        t.total_shares_purchased,
        s.close as current_close_price,
        t.total_shares_purchased * s.close as current_value,
        (t.total_shares_purchased * s.close - t.total_invested) as profit_amount,
        ((t.total_shares_purchased * s.close - t.total_invested) / t.total_invested) * 100 as return_percentage
    from {{ ref('stocks_data') }} s
    join total_costs t on s.symbol = t.symbol
    join last_trade_per_symbol l on s.symbol = l.symbol and s.trade_date = l.last_trade_date
)

select
    symbol,
    total_invested,
    total_shares_purchased,
    current_value,
    profit_amount,
    return_percentage
from current_values
order by symbol
