# ДЗ 2: CSV -> Kafka -> ClickHouse

Задание:

1. Загрузить данные из csv файла в топик Kafka (отдельный py скрипт)
2. Сформировать таблицы в ClickHouse, получающие данные из топика (отдельный sql скрипт с DDL)
3. Наприсать sql запрос для получения категории наибольшей транзакции по каждому штату (отдельный sql запрос + результирующий csv)

Файлы:

- docker-compose.yml - set up Kafka + ClickHouse
- load_to_kafka.py - п.1
- sql/01_ddl.sql - п.2
- sql/02_top_category_by_state.sql и result.csv - п.3

Папка `data/` (train.csv, test.csv, sample_submition.csv) в git не включена: train.csv весит
~138 МБ, а GitHub не принимает файлы больше 100 МБ. Чтобы запустить, положить csv в `data/` руками.

Запуск:

```
docker compose up -d
docker exec -i clickhouse clickhouse-client --multiquery < sql/01_ddl.sql
pip install -r requirements.txt
python load_to_kafka.py
docker exec -i clickhouse clickhouse-client --format CSVWithNames < sql/02_top_category_by_state.sql > result.csv
```

DDL лучше выполнить до загрузки, чтобы ClickHouse уже слушал топик.

Как устроено:

Скрипт читает `data/train.csv` и шлёт каждую строку в топик `transactions` как JSON
(числа приводим к float/int, иначе в JSON будут строками). Kafka в compose слушает на двух
портах: скрипт с хоста ходит на `localhost:29092`, а ClickHouse внутри docker-сети на `kafka:9092`.

В ClickHouse три объекта: `transactions_kafka` (движок Kafka, сама данные не хранит, просто
читает топик), `transactions` (MergeTree, тут лежат данные) и materialized view `transactions_mv`,
которая переливает из первой во вторую (плюс превращает время из строки в DateTime).
Пока стоит `ORDER BY transaction_time`, что кажется естественным и простым для первого приближения.

В запросе `argMax(cat_id, amount)` берёт категорию из строки с максимальной суммой, группировка идёт по
по `us_state`. Получилось 51 строк, в основном `travel`. Результат отдельно проверен на python.


