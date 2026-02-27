#!/usr/bin/env python3
"""
StockPulse Consumer — Redpanda → PostgreSQL sink
Consumes tick events from stock.ticks.v1 and inserts into stock_ticks table
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

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

FAILED_EVENT_SQL = """
    INSERT INTO failed_events (source, topic, partition_id, offset_id, raw_value, error_message)
    VALUES (%s, %s, %s, %s, %s, %s)
"""


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "consumer",
            "msg": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def _setup_logger(name: str) -> logging.Logger:
    _logger = logging.getLogger(name)
    _logger.setLevel(logging.INFO)
    _logger.handlers = []
    handler = logging.StreamHandler()
    handler.setFormatter(_JSONFormatter())
    _logger.addHandler(handler)
    _logger.propagate = False
    return _logger


logger = _setup_logger("consumer")


def connect_db(retries: int = 10, delay: int = 3) -> psycopg2.extensions.connection:
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            logger.info(json.dumps({"event": "db_connected", "attempt": attempt}))
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(json.dumps({"event": "db_connect_failed", "attempt": attempt, "retries": retries, "error": str(e)}))
            if attempt < retries:
                time.sleep(delay * (2 ** (attempt - 1)))
    raise RuntimeError("Could not connect to PostgreSQL after retries")


def insert_with_retry(cursor, db, tick: dict, retries: int = 3) -> None:
    for attempt in range(1, retries + 1):
        try:
            cursor.execute(INSERT_SQL, (tick["symbol"], tick["price"], tick.get("volume"), tick["event_time"]))
            db.commit()
            return
        except psycopg2.OperationalError as e:
            db.rollback()
            if attempt == retries:
                raise
            delay = 0.5 * (2 ** (attempt - 1))
            logger.warning(json.dumps({"event": "insert_retry", "attempt": attempt, "error": str(e), "delay_s": delay}))
            time.sleep(delay)


def write_to_dlq(cursor, db, msg, error: str) -> None:
    try:
        raw = msg.value().decode("utf-8", errors="replace")
        cursor.execute(FAILED_EVENT_SQL, ("consumer", msg.topic(), msg.partition(), msg.offset(), raw, error))
        db.commit()
        logger.warning(json.dumps({"event": "dlq_write", "topic": msg.topic(), "offset": msg.offset(), "error": error}))
    except Exception as dlq_err:
        db.rollback()
        logger.error(json.dumps({"event": "dlq_write_failed", "error": str(dlq_err)}))


def main():
    logger.info(json.dumps({"event": "startup", "broker": BROKER, "topic": TOPIC, "group": GROUP_ID}))

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
                logger.error(json.dumps({"event": "kafka_error", "error": str(msg.error())}))
                continue

            try:
                tick = json.loads(msg.value().decode("utf-8"))
                insert_with_retry(cursor, db, tick)
                counter += 1
                logger.info(json.dumps({"event": "tick_inserted", "count": counter, "symbol": tick["symbol"], "price": tick["price"]}))
            except (KeyError, json.JSONDecodeError) as e:
                db.rollback()
                write_to_dlq(cursor, db, msg, str(e))
            except Exception as e:
                db.rollback()
                logger.error(json.dumps({"event": "insert_error", "error": str(e)}))

    except KeyboardInterrupt:
        logger.info(json.dumps({"event": "shutdown"}))
    finally:
        consumer.close()
        cursor.close()
        db.close()


if __name__ == "__main__":
    main()
