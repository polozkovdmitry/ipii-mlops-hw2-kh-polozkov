{%- set w_fraud = var('risk_weight_fraud') -%}
{%- set w_amount = var('risk_weight_amount') -%}
{%- set w_night = var('risk_weight_night') -%}
{%- if ((w_fraud + w_amount + w_night) - 1) | abs > 0.0001 -%}
    {{ exceptions.raise_compiler_error("risk_weight_* vars must sum to 1, got " ~ (w_fraud + w_amount + w_night)) }}
{%- endif -%}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

by_customer as (
    select
        customer_id,
        any(first_name) as first_name,
        any(last_name) as last_name,
        any(gender) as gender,
        any(city) as city,
        any(state_code) as state_code,
        any(job) as job,
        min(transaction_at) as first_txn_at,
        max(transaction_at) as last_txn_at,
        count() as txn_count,
        sum(is_fraud) as fraud_count,
        maxIf(transaction_at, is_fraud = 1) as last_fraud_at,
        round(sum(amount), 2) as total_amount,
        round(avg(amount), 2) as avg_amount,
        round(max(amount), 2) as max_amount,
        topK(1)(category)[1] as main_category, -- noqa: LT01
        avg(is_night) as night_share,
        sum(is_fraud) / count() as fraud_rate
    from transactions
    group by customer_id
),

-- три компоненты скоринга, каждая в диапазоне [0, 1]
components as (
    select
        *,
        -- 1) доля фрода, с насыщением на risk_fraud_rate_cap
        least(fraud_rate / {{ var('risk_fraud_rate_cap') }}, 1) as fraud_component,
        -- 2) насколько крупные покупки относительно других клиентов (перцентиль среднего и максимума)
        (
            percent_rank() over (order by avg_amount)
            + percent_rank() over (order by max_amount)
        ) / 2 as amount_component,
        -- 3) насколько клиент "ночной" относительно других клиентов
        percent_rank() over (order by night_share) as night_component
    from by_customer
),

scored as (
    select
        *,
        round(100 * (
            {{ w_fraud }} * fraud_component
            + {{ w_amount }} * amount_component
            + {{ w_night }} * night_component
        ), 2) as risk_score
    from components
)

select
    customer_id,
    first_name,
    last_name,
    gender,
    city,
    state_code,
    job,
    first_txn_at,
    last_txn_at,
    dateDiff('day', first_txn_at, last_txn_at) + 1 as active_days,
    txn_count,
    fraud_count,
    {{ rate_pct('fraud_count', 'txn_count') }} as fraud_rate_pct,
    if(fraud_count = 0, null, last_fraud_at) as last_fraud_at,
    total_amount,
    avg_amount,
    max_amount,
    main_category,
    round(100 * night_share, 2) as night_txn_share_pct,
    round(fraud_component, 4) as fraud_component,
    round(amount_component, 4) as amount_component,
    round(night_component, 4) as night_component,
    risk_score,
    toLowCardinality(multiIf(
        risk_score >= {{ var('risk_tier_high') }}, 'HIGH',
        risk_score >= {{ var('risk_tier_medium') }}, 'MEDIUM',
        'LOW'
    )) as risk_tier
from scored
