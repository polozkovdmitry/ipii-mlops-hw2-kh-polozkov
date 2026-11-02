with transactions as (
    select * from {{ ref('stg_transactions') }}
),

overall as (
    select avg(is_fraud) as overall_fraud_rate
    from transactions
),

by_category as (
    select
        category,
        count() as txn_count,
        sum(is_fraud) as fraud_count,
        round(sum(amount), 2) as total_amount,
        round(sumIf(amount, is_fraud = 1), 2) as fraud_amount,
        round(avg(amount), 2) as avg_amount,
        round(avgIf(amount, is_fraud = 1), 2) as avg_fraud_amount
    from transactions
    group by category
)

select
    by_category.category,
    by_category.txn_count,
    by_category.fraud_count,
    {{ rate_pct('by_category.fraud_count', 'by_category.txn_count') }} as fraud_rate_pct,
    by_category.total_amount,
    by_category.fraud_amount,
    {{ rate_pct('by_category.fraud_amount', 'by_category.total_amount') }} as fraud_amount_share_pct,
    by_category.avg_amount,
    -- avgIf по пустому набору даёт nan, для категорий без фрода ставим 0
    if(by_category.fraud_count = 0, 0, by_category.avg_fraud_amount) as avg_fraud_amount,
    -- во сколько раз фрод в категории чаще, чем в среднем по всем транзакциям
    round(by_category.fraud_count / by_category.txn_count / overall.overall_fraud_rate, 2) as fraud_lift,
    toUInt8(rank() over (order by by_category.fraud_count / by_category.txn_count desc)) as fraud_rate_rank
from by_category
cross join overall
