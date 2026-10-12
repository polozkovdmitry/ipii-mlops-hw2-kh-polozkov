"""Читаем data/train.csv и отправляем каждую строку в топик Kafka как JSON."""
import csv
import json
import sys

from confluent_kafka import Producer

CSV_PATH = "data/train.csv"
TOPIC = "transactions"
BOOTSTRAP = "localhost:29092"  # внешний порт из docker-compose

# колонки, которые должны быть числами (csv читает всё как строки)
FLOAT_COLS = ["amount", "lat", "lon", "merchant_lat", "merchant_lon"]
INT_COLS = ["population_city", "target"]


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    sent = 0

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            for c in FLOAT_COLS:
                row[c] = float(row[c])
            for c in INT_COLS:
                row[c] = int(row[c])

            # если внутренняя очередь producer-а забилась - ждём, пока освободится
            while True:
                try:
                    producer.produce(TOPIC, json.dumps(row).encode("utf-8"))
                    break
                except BufferError:
                    producer.poll(0.5)

            producer.poll(0)  # обработать колбэки доставки
            sent += 1
            if sent % 100000 == 0:
                print(f"отправлено {sent} строк", file=sys.stderr)

    producer.flush()  # дождаться, пока всё реально уйдёт в брокер
    print(f"готово, всего отправлено: {sent}")


if __name__ == "__main__":
    main()
