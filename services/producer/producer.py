#!/usr/bin/env python3
"""
StockPulse Producer — Simulated real-time tick generator
Publishes OHLCV tick events to Redpanda (stock.ticks.v1)
"""

import os
import json
import time
import random
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

producer = Producer({"bootstrap.servers": BROKER})


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed for {msg.key()}: {err}", flush=True)


def generate_tick(symbol: str) -> dict:
    base = BASE_PRICES[symbol]
    drift = random.uniform(-0.5, 0.5)
    price = round(base + drift, 2)
    return {
        "symbol": symbol,
        "price": price,
        "volume": random.randint(500, 15000),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main():
    print(f"[Producer] Starting — broker={BROKER} topic={TOPIC} interval={INTERVAL}s", flush=True)
    counter = 0

    while True:
        symbol = random.choice(SYMBOLS)
        tick = generate_tick(symbol)
        producer.produce(
            topic=TOPIC,
            key=tick["symbol"],
            value=json.dumps(tick),
            callback=delivery_report,
        )
        producer.poll(0)
        counter += 1
        print(f"[Producer] #{counter} {tick['symbol']} @ {tick['price']}", flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
