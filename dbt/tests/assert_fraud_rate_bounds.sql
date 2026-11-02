-- fraud_rate_pct во всех витринах должен быть в [0, 100]
{%- set marts = [
    'mart_daily_state_metrics',
    'mart_fraud_by_category',
    'mart_fraud_by_state',
    'mart_customer_risk_profile',
    'mart_hourly_fraud_pattern',
    'mart_merchant_analytics',
] %}

{% for mart in marts %}
    select
        '{{ mart }}' as mart,
        fraud_rate_pct
    from {{ ref(mart) }}
    where fraud_rate_pct > 100 or fraud_rate_pct < 0
    {% if not loop.last %}union all{% endif %}
{% endfor %}
