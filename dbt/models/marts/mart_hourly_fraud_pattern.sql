with transactions as (
    select * from {{ ref('stg_transactions') }}
),

overall as (
    select avg(is_fraud) as overall_fraud_rate
    from transactions
),

by_slot as (
    select
        day_of_week,
        any(day_name) as day_name,
        hour_of_day,
        count() as txn_count,
        sum(is_fraud) as fraud_count,
        round(avg(amount), 2) as avg_amount,
        round(sumIf(amount, is_fraud = 1), 2) as fraud_amount
    from transactions
    group by day_of_week, hour_of_day
)

select
    by_slot.day_of_week,
    by_slot.day_name,
    by_slot.hour_of_day,
    toUInt8({{ is_night_hour('by_slot.hour_of_day') }}) as is_night,
    by_slot.txn_count,
    by_slot.fraud_count,
    {{ rate_pct('by_slot.fraud_count', 'by_slot.txn_count') }} as fraud_rate_pct,
    by_slot.avg_amount,
    by_slot.fraud_amount,
    round(by_slot.fraud_count / by_slot.txn_count / overall.overall_fraud_rate, 2) as fraud_lift,
    -- окно повышенного риска: фрод в слоте в high_risk_window_lift раз чаще среднего
    toUInt8(
        by_slot.fraud_count / by_slot.txn_count
        >= {{ var('high_risk_window_lift') }} * overall.overall_fraud_rate
    ) as is_high_risk_window,
    toUInt8(rank() over (order by by_slot.fraud_count / by_slot.txn_count desc)) as fraud_rate_rank
from by_slot
cross join overall
