with source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        toDateTime(transaction_time) as transaction_at,
        -- у всех мерчантов в датасете синтетический префикс fraud_, он не связан с фродом
        replaceRegexpOne(merch, '^fraud_', '') as merchant_name,
        toLowCardinality(cat_id) as category,
        toFloat64(amount) as amount,
        trimBoth(name_1) as first_name,
        trimBoth(name_2) as last_name,
        toLowCardinality(upper(gender)) as gender,
        street,
        one_city as city,
        toLowCardinality(upper(us_state)) as state_code,
        post_code,
        toFloat64(lat) as customer_lat,
        toFloat64(lon) as customer_lon,
        toUInt32(population_city) as city_population,
        jobs as job,
        toFloat64(merchant_lat) as merchant_lat,
        toFloat64(merchant_lon) as merchant_lon,
        toUInt8(target) as is_fraud
    from source
),

enriched as (
    select
        -- id транзакции: в источнике его нет, но комбинация полей ниже уникальна
        {{ dbt_utils.generate_surrogate_key([
            'transaction_at', 'merchant_name', 'category', 'amount',
            'first_name', 'last_name', 'street', 'post_code'
        ]) }} as transaction_id,
        -- клиент = имя + адрес (одних имени и фамилии не хватает, есть тёзки)
        {{ dbt_utils.generate_surrogate_key([
            'first_name', 'last_name', 'street', 'post_code'
        ]) }} as customer_id,
        transaction_at,
        toDate(transaction_at) as transaction_date,
        toHour(transaction_at) as hour_of_day,
        {{ dbt_date.day_of_week('transaction_at', isoweek=true) }} as day_of_week,
        {{ dbt_date.day_name('transaction_at', short=false) }} as day_name,
        toUInt8({{ is_night_hour('toHour(transaction_at)') }}) as is_night,
        merchant_name,
        category,
        amount,
        toLowCardinality({{ amount_bucket('amount') }}) as amount_bucket,
        toUInt8(amount >= {{ var('large_amount_threshold') }}) as is_large_amount,
        first_name,
        last_name,
        gender,
        street,
        city,
        state_code,
        post_code,
        customer_lat,
        customer_lon,
        city_population,
        job,
        merchant_lat,
        merchant_lon,
        is_fraud
    from renamed
)

select * from enriched
