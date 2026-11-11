import json
import logging
import os
import sys
import time
from collections import deque

import pandas as pd
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

sys.path.append(os.path.abspath('./src'))  # noqa: PTH100
from preprocessing import run_preproc
from scorer import ensure_bootstrap, make_pred


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
# доля фрода считается по последним N транзакциям, а не с момента старта
FRAUD_WINDOW = int(os.getenv("FRAUD_RATIO_WINDOW", "1000"))

# поля транзакции, которые идут дальше в топик scoring вместе со скором (для срезов в Grafana)
CONTEXT_FIELDS = ["us_state", "merch", "cat_id", "amount", "target"]

SCORE_BUCKETS = [i / 50 for i in range(51)]
# обычно обработка 5-30 мс, бакеты с запасом до секунды
LATENCY_BUCKETS = [0.002, 0.005, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075, 0.1, 0.2, 0.5, 1.0]

TRANSACTIONS = Counter(
    'scorer_transactions_total', 'Обработанные транзакции',
)
FLAGGED = Counter(
    'scorer_fraud_flagged_total', 'Транзакции, помеченные как фрод',
)
ERRORS = Counter(
    'scorer_errors_total', 'Сообщения, которые не удалось обработать',
)
PROCESSING_TIME = Histogram(
    'scorer_processing_seconds',
    'Время обработки одной транзакции: препроцессинг, инференс, отправка в Kafka',
    buckets=LATENCY_BUCKETS,
)
SCORES = Histogram(
    'scorer_score', 'Распределение скоров', buckets=SCORE_BUCKETS,
)
FRAUD_RATIO = Gauge(
    'scorer_fraud_ratio', f'Доля фрода среди последних {FRAUD_WINDOW} транзакций',
)
EVENT_LAG = Gauge(
    'scorer_event_lag_seconds',
    'Сколько прошло от записи транзакции в Kafka до выдачи скора',
)
MODEL_SWITCHES = Counter(
    'scorer_model_switches_total',
    'Переключения на новую версию модели без перезапуска',
    ['version'],
)
MODEL_INFO = Gauge(
    'scorer_model_info', 'Текущая версия модели (значение всегда 1)', ['version'],
)


class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
        })
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.recent_flags = deque(maxlen=FRAUD_WINDOW)
        self.model_version = None
        start_http_server(METRICS_PORT)
        logger.info("Метрики Prometheus на порту %s", METRICS_PORT)

    def _track_model_version(self, version):
        if version == self.model_version:
            return
        if self.model_version is None:
            # первая модель после старта контейнера - это не смена версии
            logger.info("Скоринг стартовал на модели %s", version)
        else:
            MODEL_SWITCHES.labels(version=version).inc()
            MODEL_INFO.remove(self.model_version)
            logger.info("Модель сменилась: %s -> %s", self.model_version, version)
        MODEL_INFO.labels(version=version).set(1)
        self.model_version = version

    def process_message(self, msg):
        started = time.perf_counter()
        try:
            data = json.loads(msg.value().decode('utf-8'))
            transaction_id = data['transaction_id']
            row = data['data']

            processed_df = run_preproc(pd.DataFrame([row]))
            _, y_proba, model_version = make_pred(processed_df, "kafka_stream")
            score = float(y_proba[0])
            fraud_flag = int(score > 0.5)

            result = {
                'transaction_id': transaction_id,
                'score': score,
                'fraud_flag': fraud_flag,
                'model_version': model_version,
            }
            result.update({k: row.get(k) for k in CONTEXT_FIELDS})
            # список из одного элемента - как и раньше, scoring_writer ждёт этот формат
            self.producer.produce(SCORING_TOPIC, value=json.dumps([result]))
            self.producer.flush()
        except Exception:
            ERRORS.inc()
            logger.exception("Ошибка обработки сообщения")
            return

        TRANSACTIONS.inc()
        SCORES.observe(score)
        FLAGGED.inc(fraud_flag)
        self.recent_flags.append(fraud_flag)
        FRAUD_RATIO.set(sum(self.recent_flags) / len(self.recent_flags))
        self._track_model_version(model_version)
        PROCESSING_TIME.observe(time.perf_counter() - started)

        ts_type, ts_ms = msg.timestamp()
        if ts_ms > 0:
            EVENT_LAG.set(max(time.time() - ts_ms / 1000, 0))

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            self.process_message(msg)


if __name__ == "__main__":
    logger.info('Запуск сервиса скоринга')
    ensure_bootstrap()
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Остановлен')
