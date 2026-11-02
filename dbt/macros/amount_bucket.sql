{#
    Сегментация суммы транзакции по фиксированным границам в долларах.
    cutoffs - верхние (не включительно) границы первых четырёх бакетов,
    пятый бакет открыт сверху. По умолчанию берутся из var amount_bucket_cutoffs.

    {{ amount_bucket('amount') }}                     -> micro / small / medium / large / very_large
    {{ amount_bucket('amount', [5, 25, 100, 500]) }}  -> те же названия, другие границы
#}
{% macro amount_bucket(amount_col, cutoffs=none) %}
    {%- set cutoffs = cutoffs if cutoffs is not none else var('amount_bucket_cutoffs') -%}
    {%- set labels = ['micro', 'small', 'medium', 'large'] -%}
    {%- if cutoffs | length != labels | length -%}
        {{ exceptions.raise_compiler_error("amount_bucket expects 4 cutoffs, got " ~ cutoffs | length) }}
    {%- endif -%}
    multiIf(
        {%- for cutoff in cutoffs %}
        {{ amount_col }} < {{ cutoff }}, '{{ labels[loop.index0] }}',
        {%- endfor %}
        'very_large'
    )
{% endmacro %}
