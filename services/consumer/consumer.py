#!/usr/bin/env python3
"""
StockPulse Consumer — Redpanda → PostgreSQL sink
Consumes tick events from stock.ticks.v1 and inserts into stock_ticks table
"""

import os
import json
import time
import psycopg2
from confluent_kafka import Consumer, KafkaError

BROKER = os.getenv("KAFKA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC_STOCK_TICKS", "stock.ticks.v1")
GROUP_ID = "stockpulse-consumer-v1"

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "stockpulse"),
    "user": os.getenv("POSTGRES_USER", "stockpulse"),
    "password": os.getenv("POSTGRES_PASSWORD", "stockpulse_pass"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
}

INSERT_SQL = """
    INSERT INTO stock_ticks (symbol, price, volume, event_time)
    VALUES (%s, %s, %s, %s)
"""


def connect_db(retries: int = 10, delay: int = 3) -> psycopg2.extensions.connection:
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print(f"[Consumer] DB connected on attempt {attempt}", flush=True)
            return conn
        except psycopg2.OperationalError as e:
            print(f"[Consumer] DB connection attempt {attempt}/{retries} failed: {e}", flush=True)
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL after retries")


def main():
    print(f"[Consumer] Starting — broker={BROKER} topic={TOPIC} group={GROUP_ID}", flush=True)

    db = connect_db()
    cursor = db.cursor()

    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])

    counter = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[Consumer] Kafka error: {msg.error()}", flush=True)
                continue

            try:
                tick = json.loads(msg.value().decode("utf-8"))
                cursor.execute(INSERT_SQL, (
                    tick["symbol"],
                    tick["price"],
                    tick.get("volume"),
                    tick["event_time"],
                ))
                db.commit()
                counter += 1
                print(f"[Consumer] #{counter} inserted {tick['symbol']} @ {tick['price']}", flush=True)
            except (KeyError, json.JSONDecodeError) as e:
                print(f"[Consumer] Bad message, skipping: {e}", flush=True)
                db.rollback()

    except KeyboardInterrupt:
        print("[Consumer] Shutting down", flush=True)
    finally:
        consumer.close()
        cursor.close()
        db.close()


if __name__ == "__main__":
    main()
