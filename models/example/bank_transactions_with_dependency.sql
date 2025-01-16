select
    {{ dbt_utils.star(from=ref('bank_transactions')) }}
from {{ ref('bank_transactions') }}