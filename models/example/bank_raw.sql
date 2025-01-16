select * from
        {{ source('bank_transactions', 'stocks') }}

