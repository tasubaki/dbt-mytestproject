{% set fields = ['date', 'withdrawal', 'deposit'] %}

select
    {% for field in fields %}
    {{ field }} 
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
from {{ ref('bank_transactions') }}