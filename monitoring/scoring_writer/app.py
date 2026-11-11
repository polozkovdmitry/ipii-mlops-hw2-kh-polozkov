import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
SCORE_BUCKETS = [i / 50 for i in range(51)]
WRITE_BUCKETS = [0.0005, 0.001, 0.002, 0.003, 0.005, 0.0075, 0.01, 0.02, 0.05, 0.1, 0.5]

WRITTEN = Counter('writer_scores_written_total', 'Скоры, записанные в PostgreSQL')
FRAUD_WRITTEN = Counter('writer_fraud_flagged_total', 'Записанные транзакции с флагом фрода')
ERRORS = Counter('writer_errors_total', 'Сообщения, которые не удалось записать')
WRITE_TIME = Histogram('writer_db_write_seconds', 'Время INSERT + COMMIT одного скора', buckets=WRITE_BUCKETS)
WRITTEN_SCORES = Histogram('writer_score', 'Распределение записанных скоров', buckets=SCORE_BUCKETS)

REQUIRED_KEYS = ('transaction_id', 'score', 'fraud_flag')


def connect_db():
    config = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    for attempt in range(30):
        try:
            return psycopg2.connect(**config)
        except psycopg2.OperationalError as exc:
            logger.info("Postgres ещё не готов (%s): %s", attempt + 1, exc)
            time.sleep(2)
    raise RuntimeError("Не удалось подключиться к Postgres")


def create_table(conn):
    # us_state, merch, cat_id, amount и target нужны только для срезов в Grafana
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scores (
                id BIGSERIAL PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                fraud_flag SMALLINT NOT NULL,
                model_version TEXT,
                us_state TEXT,
                merch TEXT,
                cat_id TEXT,
                amount DOUBLE PRECISION,
                target SMALLINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS scores_created_at_idx ON scores (created_at);
        """)
    conn.commit()
    logger.info("Таблица scores готова")


def insert_score(conn, data):
    started = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scores (transaction_id, score, fraud_flag, model_version,
                                us_state, merch, cat_id, amount, target)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                data["transaction_id"], data["score"], data["fraud_flag"], data.get("model_version"),
                data.get("us_state"), data.get("merch"), data.get("cat_id"), data.get("amount"),
                data.get("target"),
            ),
        )
    conn.commit()
    WRITE_TIME.observe(time.perf_counter() - started)

    WRITTEN.inc()
    WRITTEN_SCORES.observe(data["score"])
    if data["fraud_flag"] == 1:
        FRAUD_WRITTEN.inc()


def run_consumer():
    start_http_server(METRICS_PORT)
    logger.info("Метрики Prometheus на порту %s", METRICS_PORT)

    consumer = Consumer({
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'group.id': 'scoring-writer',
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([os.getenv("KAFKA_SCORING_TOPIC")])

    conn = connect_db()
    create_table(conn)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            try:
                items = json.loads(msg.value().decode('utf-8'))
                if isinstance(items, dict):
                    items = [items]
                for data in items:
                    if not all(k in data for k in REQUIRED_KEYS):
                        ERRORS.inc()
                        logger.error("Некорректное сообщение: %s", data)
                        continue
                    insert_score(conn, data)
            except Exception:
                ERRORS.inc()
                conn.rollback()
                logger.exception("Ошибка обработки сообщения")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    run_consumer()
