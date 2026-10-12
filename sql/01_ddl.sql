-- DDL для ClickHouse. Схема: Kafka -> (materialized view) -> MergeTree

CREATE DATABASE IF NOT EXISTS hw2;

-- 1. Таблица с движком Kafka. Сама данные не хранит, только читает топик.
CREATE TABLE IF NOT EXISTS hw2.transactions_kafka
(
    transaction_time String,   -- в csv формат '2019-12-27 15:21', поэтому пока строка
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        String,
    lat              Float64,
    lon              Float64,
    population_city  UInt32,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',   -- внутренний адрес из docker-сети
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- 2. Таблица, где данные лежат по-настоящему (оптимизированная версия, п.4).
--    Что поменяли: ORDER BY transaction_time -> us_state и LowCardinality для
--    колонок с малым числом разных значений (штаты, категории, пол).
CREATE TABLE IF NOT EXISTS hw2.transactions
(
    transaction_time DateTime,
    merch            String,
    cat_id           LowCardinality(String),
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           LowCardinality(String),
    street           String,
    one_city         String,
    us_state         LowCardinality(String),
    post_code        String,
    lat              Float64,
    lon              Float64,
    population_city  UInt32,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           UInt8
)
ENGINE = MergeTree
-- сортируем по штату: запрос группирует по us_state, одинаковые штаты лежат рядом
ORDER BY us_state;

-- 3. Materialized view: как только в Kafka-таблицу приходят сообщения,
--    они переливаются в transactions (заодно превращаем время в DateTime).
CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.transactions_mv
TO hw2.transactions AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch, cat_id, amount, name_1, name_2, gender, street, one_city,
    us_state, post_code, lat, lon, population_city, jobs,
    merchant_lat, merchant_lon, target
FROM hw2.transactions_kafka;
