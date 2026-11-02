{#
    Общие формулы для витрин.
#}

{# Доля в процентах с защитой от деления на ноль, округление до 4 знаков. #}
{% macro rate_pct(numerator, denominator) %}
    if({{ denominator }} = 0, 0, round(100.0 * {{ numerator }} / {{ denominator }}, 4))
{% endmacro %}


{#
    Нижняя граница доверительного интервала Уилсона для доли успехов (в долях, не в %).
    Даёт консервативную оценку fraud rate: у мерчанта с 1 фродом из 2 транзакций
    граница низкая, у мерчанта с 40 из 200 - высокая.
    https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Wilson_score_interval
#}
{% macro wilson_lower_bound(successes, trials, z=none) %}
    {%- set z = z if z is not none else var('wilson_z') -%}
    if(
        {{ trials }} = 0,
        0,
        (
            ({{ successes }} / {{ trials }}) + {{ z }} * {{ z }} / (2 * {{ trials }})
            - {{ z }} * sqrt(
                ({{ successes }} / {{ trials }}) * (1 - {{ successes }} / {{ trials }}) / {{ trials }}
                + {{ z }} * {{ z }} / (4 * {{ trials }} * {{ trials }})
            )
        ) / (1 + {{ z }} * {{ z }} / {{ trials }})
    )
{% endmacro %}


{# Попадает ли час в ночное окно [night_start_hour, night_end_hour), окно переходит через полночь. #}
{% macro is_night_hour(hour_col) %}
    ({{ hour_col }} >= {{ var('night_start_hour') }} or {{ hour_col }} < {{ var('night_end_hour') }})
{% endmacro %}
