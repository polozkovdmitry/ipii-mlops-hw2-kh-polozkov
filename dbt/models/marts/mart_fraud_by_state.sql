with transactions as (
    select * from {{ ref('stg_transactions') }}
),

states as (
    select * from {{ ref('states') }}
),

by_state as (
    select
        state_code,
        count() as txn_count,
        sum(is_fraud) as fraud_count,
        uniqExact(customer_id) as unique_customers,
        uniqExactIf(customer_id, is_fraud = 1) as fraud_customers,
        uniqExact(merchant_name) as unique_merchants,
        round(sum(amount), 2) as total_amount,
        round(sumIf(amount, is_fraud = 1), 2) as fraud_amount
    from transactions
    group by state_code
)

select
    by_state.state_code,
    states.state_name,
    states.region,
    states.division,
    by_state.txn_count,
    by_state.fraud_count,
    {{ rate_pct('by_state.fraud_count', 'by_state.txn_count') }} as fraud_rate_pct,
    by_state.unique_customers,
    by_state.fraud_customers,
    {{ rate_pct('by_state.fraud_customers', 'by_state.unique_customers') }} as fraud_customer_share_pct,
    by_state.unique_merchants,
    by_state.total_amount,
    by_state.fraud_amount,
    {{ rate_pct('by_state.fraud_amount', 'by_state.total_amount') }} as fraud_amount_share_pct,
    toUInt8(rank() over (order by by_state.fraud_count / by_state.txn_count desc)) as fraud_rate_rank
from by_state
left join states on by_state.state_code = states.state_code
