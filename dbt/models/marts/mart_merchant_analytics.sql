with transactions as (
    select * from {{ ref('stg_transactions') }}
),

overall as (
    select avg(is_fraud) as overall_fraud_rate
    from transactions
),

by_merchant as (
    select
        merchant_name,
        topK(1)(category)[1] as main_category, -- noqa: LT01
        uniqExact(category) as category_count,
        count() as txn_count,
        sum(is_fraud) as fraud_count,
        round(sum(amount), 2) as turnover,
        round(sumIf(amount, is_fraud = 1), 2) as fraud_amount,
        round(avg(amount), 2) as avg_amount,
        uniqExact(customer_id) as unique_customers,
        min(transaction_at) as first_txn_at,
        max(transaction_at) as last_txn_at
    from transactions
    group by merchant_name
),

scored as (
    select
        by_merchant.*,
        overall.overall_fraud_rate,
        {{ wilson_lower_bound('by_merchant.fraud_count', 'by_merchant.txn_count') }} as fraud_rate_lower_bound
    from by_merchant
    cross join overall
)

select
    merchant_name,
    main_category,
    category_count,
    txn_count,
    fraud_count,
    {{ rate_pct('fraud_count', 'txn_count') }} as fraud_rate_pct,
    round(100 * fraud_rate_lower_bound, 4) as fraud_rate_lower_bound_pct,
    round(100 * overall_fraud_rate, 4) as overall_fraud_rate_pct,
    turnover,
    fraud_amount,
    avg_amount,
    unique_customers,
    first_txn_at,
    last_txn_at,
    -- подозрительный мерчант: даже пессимистичная (нижняя 95%) оценка его fraud rate
    -- выше общего уровня. Шум у мелких мерчантов так не срабатывает.
    toUInt8(fraud_rate_lower_bound > overall_fraud_rate) as is_suspicious
from scored
