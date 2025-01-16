WITH stocks_raw AS (
    SELECT
        symbol,
        date_trunc('day', time) as trade_date,
        open,
        high,
        low,
        close,
        volume
    FROM {{ source('bank_transactions', 'stocks') }}
)

SELECT * FROM stocks_raw