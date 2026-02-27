#!/usr/bin/env python3
"""
StockPulse Producer — Simulated real-time tick generator
Publishes OHLCV tick events to Redpanda (stock.ticks.v1)
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BROKER = os.getenv("KAFKA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC_STOCK_TICKS", "stock.ticks.v1")
INTERVAL = float(os.getenv("PRODUCE_INTERVAL", "2"))

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"]

BASE_PRICES = {
    "AAPL": 190.0,
    "MSFT": 415.0,
    "GOOG": 175.0,
    "AMZN": 185.0,
    "TSLA": 245.0,
    "NVDA": 875.0,
}


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "producer",
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


logger = _setup_logger("producer")

kafka_producer = Producer({"bootstrap.servers": BROKER})


def delivery_report(err, msg):
    if err:
        logger.error(json.dumps({"event": "delivery_failed", "topic": msg.topic(), "error": str(err)}))


def generate_tick(symbol: str) -> dict:
    base = BASE_PRICES[symbol]
    drift = random.uniform(-0.5, 0.5)
    return {
        "symbol": symbol,
        "price": round(base + drift, 2),
        "volume": random.randint(500, 15000),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main():
    logger.info(json.dumps({"event": "startup", "broker": BROKER, "topic": TOPIC, "interval_s": INTERVAL}))
    counter = 0

    while True:
        symbol = random.choice(SYMBOLS)
        tick = generate_tick(symbol)
        kafka_producer.produce(
            topic=TOPIC,
            key=tick["symbol"],
            value=json.dumps(tick),
            callback=delivery_report,
        )
        kafka_producer.poll(0)
        counter += 1
        logger.info(json.dumps({"event": "tick_produced", "count": counter, "symbol": tick["symbol"], "price": tick["price"]}))
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
