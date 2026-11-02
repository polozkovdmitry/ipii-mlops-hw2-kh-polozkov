-- Суммы транзакций не могут быть отрицательными
select
    transaction_id,
    amount
from {{ ref('stg_transactions') }}
where amount < 0
