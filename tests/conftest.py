"""
pytest configuration and fixtures for StockPulse ETL test suite
"""

import os
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker


@pytest.fixture()
def faker():
    """Faker instance for generating test data."""
    return Faker()


@pytest.fixture()
def sample_tick():
    """Sample stock tick for testing."""
    return {
        "symbol": "AAPL",
        "open": 190.25,
        "high": 191.50,
        "low": 189.75,
        "close": 190.80,
        "volume": 52400000,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "yfinance",
    }


@pytest.fixture()
def sample_ticks(sample_tick):
    """Multiple sample ticks for different symbols."""
    symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"]
    ticks = []
    for i, symbol in enumerate(symbols):
        tick = sample_tick.copy()
        tick["symbol"] = symbol
        tick["close"] = 100.0 + i * 10
        ticks.append(tick)
    return ticks


@pytest.fixture()
def sample_bar():
    """Sample 1-minute OHLCV bar."""
    return {
        "symbol": "AAPL",
        "bucket_start": "2026-02-27 10:00:00",
        "open": 190.25,
        "high": 191.50,
        "low": 189.75,
        "close": 190.80,
        "volume": 52400000,
    }


@pytest.fixture()
def sample_bars(sample_bar):
    """Multiple sample bars for different symbols."""
    symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"]
    bars = []
    for i, symbol in enumerate(symbols):
        bar = sample_bar.copy()
        bar["symbol"] = symbol
        bar["close"] = 100.0 + i * 10
        bars.append(bar)
    return bars


@pytest.fixture()
def mock_kafka_producer():
    """Mock Kafka producer."""
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.poll = MagicMock()
    producer.flush = MagicMock()
    return producer


@pytest.fixture()
def mock_db_connection():
    """Mock database connection."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.execute = MagicMock()
    cursor.fetchone = MagicMock(return_value=(1,))
    cursor.fetchall = MagicMock(return_value=[])
    cursor.close = MagicMock()
    conn.commit = MagicMock()
    conn.close = MagicMock()
    return conn


@pytest.fixture()
def env_vars(monkeypatch):
    """Set up required environment variables."""
    monkeypatch.setenv("KAFKA_BROKERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC_STOCK_TICKS", "stock.ticks.v1")
    monkeypatch.setenv("KAFKA_TOPIC_STOCK_BARS", "stock.bars.v1")
    monkeypatch.setenv("PRODUCE_INTERVAL", "2")
    monkeypatch.setenv("PRODUCER_MODE", "cached")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_USER", "postgres")
    monkeypatch.setenv("DB_PASSWORD", "postgres")
    monkeypatch.setenv("DB_NAME", "stockpulse")
    return monkeypatch


@pytest.fixture()
def valid_tick_json(sample_tick):
    """Valid tick as JSON string."""
    return json.dumps(sample_tick)


@pytest.fixture()
def invalid_tick_json():
    """Invalid tick JSON (missing required fields)."""
    return json.dumps({
        "symbol": "AAPL",
        # Missing: open, high, low, close, volume, event_time
    })


@pytest.fixture()
def valid_bar_json(sample_bar):
    """Valid bar as JSON string."""
    return json.dumps(sample_bar)
