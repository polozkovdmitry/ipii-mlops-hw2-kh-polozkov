-- risk_score в [0, 100] и согласован с risk_tier.
-- Если веса amount + night не дотягивают до порога HIGH (так при значениях по умолчанию),
-- то клиент без фрода не может попасть в HIGH - это тоже проверяем.
{%- set non_fraud_max = 100 * (var('risk_weight_amount') + var('risk_weight_night')) %}

select
    customer_id,
    risk_score,
    risk_tier,
    fraud_count
from {{ ref('mart_customer_risk_profile') }}
where risk_score < 0
    or risk_score > 100
    or risk_tier != multiIf(
        risk_score >= {{ var('risk_tier_high') }}, 'HIGH',
        risk_score >= {{ var('risk_tier_medium') }}, 'MEDIUM',
        'LOW'
    )
    {% if non_fraud_max < var('risk_tier_high') %}
        or (risk_tier = 'HIGH' and fraud_count = 0)
    {% endif %}
