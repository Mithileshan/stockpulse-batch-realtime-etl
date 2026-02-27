#!/usr/bin/env python3
"""
StockPulse Producer (Real Data) — yFinance-powered real-time tick generator
Fetches live stock data from yFinance and publishes OHLCV events to Redpanda (stock.ticks.v1)
Default: low-latency mode (2s interval, last cached price)
Can be switched to polling mode for fresh data on each tick.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import yfinance as yf
from confluent_kafka import Producer

BROKER = os.getenv("KAFKA_BROKERS", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC_STOCK_TICKS", "stock.ticks.v1")
INTERVAL = float(os.getenv("PRODUCE_INTERVAL", "2"))
MODE = os.getenv("PRODUCER_MODE", "cached")  # 'cached' or 'poll'

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"]


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "producer_yfinance",
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


logger = _setup_logger("producer_yfinance")

kafka_producer = Producer({"bootstrap.servers": BROKER})

# Cache for yfinance data
_cached_data = {}


def delivery_report(err, msg):
    if err:
        logger.error(json.dumps({"event": "delivery_failed", "topic": msg.topic(), "error": str(err)}))


def fetch_tickers_data():
    """Fetch latest price data for all symbols via yFinance."""
    try:
        # Download latest price data for all symbols
        data = yf.download(
            " ".join(SYMBOLS),
            period="1d",
            interval="1m",
            progress=False,
            group_by="ticker",
        )
        return data
    except Exception as e:
        logger.error(json.dumps({"event": "fetch_error", "error": str(e)}))
        return None


def get_cached_tick(symbol: str, fresh_data) -> dict:
    """
    Get the latest tick from cached data.
    Updates cache on each fetch.
    """
    try:
        if fresh_data is not None and symbol in fresh_data.columns.get_level_values(0).unique():
            # Extract latest row for symbol
            symbol_data = fresh_data[symbol]
            if isinstance(symbol_data, type(None)):
                raise ValueError(f"No data for {symbol}")
            
            # Get the last row (most recent)
            latest = symbol_data.iloc[-1] if len(symbol_data) > 0 else None
            if latest is None or latest[["Open", "High", "Low", "Close"]].isna().any():
                # Fall back to cache if data incomplete
                if symbol in _cached_data:
                    return _cached_data[symbol]
                raise ValueError(f"Incomplete data for {symbol}")

            tick = {
                "symbol": symbol,
                "open": round(float(latest["Open"]), 2),
                "high": round(float(latest["High"]), 2),
                "low": round(float(latest["Low"]), 2),
                "close": round(float(latest["Close"]), 2),
                "volume": int(latest["Volume"]),
                "event_time": datetime.now(timezone.utc).isoformat(),
                "source": "yfinance",
            }
            _cached_data[symbol] = tick
            return tick
        elif symbol in _cached_data:
            return _cached_data[symbol]
        else:
            raise ValueError(f"No cached data for {symbol}")
    except Exception as e:
        logger.error(json.dumps({"event": "cache_error", "symbol": symbol, "error": str(e)}))
        if symbol in _cached_data:
            return _cached_data[symbol]
        return None


def get_polled_tick(symbol: str) -> dict:
    """
    Fetch fresh tick on each call (higher latency, always fresh).
    Best for high-fidelity backtesting.
    """
    try:
        data = yf.download(symbol, period="1d", interval="1m", progress=False)
        if data is None or len(data) == 0:
            if symbol in _cached_data:
                return _cached_data[symbol]
            raise ValueError(f"No data for {symbol}")
        
        latest = data.iloc[-1]
        if latest[["Open", "High", "Low", "Close"]].isna().any():
            if symbol in _cached_data:
                return _cached_data[symbol]
            raise ValueError(f"Incomplete data for {symbol}")

        tick = {
            "symbol": symbol,
            "open": round(float(latest["Open"]), 2),
            "high": round(float(latest["High"]), 2),
            "low": round(float(latest["Low"]), 2),
            "close": round(float(latest["Close"]), 2),
            "volume": int(latest["Volume"]),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "source": "yfinance_poll",
        }
        _cached_data[symbol] = tick
        return tick
    except Exception as e:
        logger.error(json.dumps({"event": "poll_error", "symbol": symbol, "error": str(e)}))
        if symbol in _cached_data:
            return _cached_data[symbol]
        return None


def main():
    logger.info(
        json.dumps({
            "event": "startup",
            "broker": BROKER,
            "topic": TOPIC,
            "interval_s": INTERVAL,
            "mode": MODE,
            "symbols": SYMBOLS,
        })
    )
    
    counter = 0
    fetch_counter = 0
    
    # Pre-cache on startup
    if MODE == "cached":
        logger.info(json.dumps({"event": "initial_fetch", "symbols": SYMBOLS}))
        fresh_data = fetch_tickers_data()
    
    while True:
        try:
            if MODE == "cached":
                # Refresh cache every 60 seconds
                if counter % 30 == 0:  # Assuming INTERVAL=2, every 60 seconds
                    fresh_data = fetch_tickers_data()
                    fetch_counter += 1
                    logger.info(json.dumps({
                        "event": "cache_refresh",
                        "fetch_count": fetch_counter,
                    }))
                
                # Rotate through symbols from cache
                for symbol in SYMBOLS:
                    tick = get_cached_tick(symbol, fresh_data)
                    if tick:
                        kafka_producer.produce(
                            topic=TOPIC,
                            key=tick["symbol"],
                            value=json.dumps(tick),
                            callback=delivery_report,
                        )
                        kafka_producer.poll(0)
                        counter += 1
                        logger.info(json.dumps({
                            "event": "tick_produced",
                            "count": counter,
                            "symbol": tick["symbol"],
                            "close": tick["close"],
                            "source": "yfinance_cached",
                        }))
            
            elif MODE == "poll":
                # Fetch fresh on each cycle
                for symbol in SYMBOLS:
                    tick = get_polled_tick(symbol)
                    if tick:
                        kafka_producer.produce(
                            topic=TOPIC,
                            key=tick["symbol"],
                            value=json.dumps(tick),
                            callback=delivery_report,
                        )
                        kafka_producer.poll(0)
                        counter += 1
                        logger.info(json.dumps({
                            "event": "tick_produced",
                            "count": counter,
                            "symbol": tick["symbol"],
                            "close": tick["close"],
                            "source": "yfinance_poll",
                        }))
            
            time.sleep(INTERVAL)
        
        except KeyboardInterrupt:
            logger.info(json.dumps({"event": "shutdown", "total_ticks": counter}))
            break
        except Exception as e:
            logger.error(json.dumps({"event": "main_loop_error", "error": str(e)}))
            time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
