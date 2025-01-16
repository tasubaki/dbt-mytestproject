select
{{ avg_field('deposit') }}
from {{ ref('bank_transactions')}}