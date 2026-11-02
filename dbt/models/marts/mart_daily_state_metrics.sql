{{
    config(
        materialized='incremental',
        incremental_strategy='delete_insert',
        unique_key='transaction_date, state_code',
        order_by='(state_code, transaction_date)',
        partition_by='toYYYYMM(transaction_date)'
    )
}}

-- Инкрементальная витрина: при повторном запуске пересчитываются только последние
-- incremental_lookback_days дней (поздно пришедшие транзакции попадут в пересчёт),
-- строки за эти дни удаляются и вставляются заново (delete_insert по ключу дата + штат).

with transactions as (
    select * from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        where transaction_date >= (
                select max(current_mart.transaction_date) - {{ var('incremental_lookback_days') }}
                from {{ this }} as current_mart
            )
    {% endif %}
)

select
    transaction_date,
    state_code,
    count() as txn_count,
    round(sum(amount), 2) as total_amount,
    round(avg(amount), 2) as avg_amount,
    round(quantile(0.95)(amount), 2) as p95_amount, -- noqa: LT01
    sum(is_large_amount) as large_txn_count,
    {{ rate_pct('sum(is_large_amount)', 'count()') }} as large_txn_share_pct,
    sum(is_fraud) as fraud_count,
    {{ rate_pct('sum(is_fraud)', 'count()') }} as fraud_rate_pct,
    now() as updated_at
from transactions
group by transaction_date, state_code
