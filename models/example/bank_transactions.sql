with txs as (
    select * from {{ ref('bank_raw') }}
)

select
    "Account No" as account_no,
    "DATE" as date,
    "TRANSACTION DETAILS" as transaction_details,
    "WITHDRAWAL AMT" as withdrawal,
    "DEPOSIT AMT" as deposit,
    "BALANCE AMT" as balance
from txs
