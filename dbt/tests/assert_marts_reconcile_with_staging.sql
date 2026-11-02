-- Каждая витрина - это разрез одних и тех же транзакций, поэтому сумма txn_count и
-- fraud_count по любой из них должна совпадать со staging. Ловит потерю строк в join-ах
-- и дубли в инкрементальной витрине.
{%- set marts = [
    'mart_daily_state_metrics',
    'mart_fraud_by_category',
    'mart_fraud_by_state',
    'mart_customer_risk_profile',
    'mart_hourly_fraud_pattern',
    'mart_merchant_analytics',
] %}

with expected as (
    select
        count() as txn_count,
        sum(is_fraud) as fraud_count
    from {{ ref('stg_transactions') }}
),

actual as (
    {% for mart in marts %}
        select
            '{{ mart }}' as mart,
            sum(txn_count) as txn_count,
            sum(fraud_count) as fraud_count
        from {{ ref(mart) }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    actual.mart,
    actual.txn_count,
    expected.txn_count as expected_txn_count,
    actual.fraud_count,
    expected.fraud_count as expected_fraud_count
from actual
cross join expected
where actual.txn_count != expected.txn_count
    or actual.fraud_count != expected.fraud_count
