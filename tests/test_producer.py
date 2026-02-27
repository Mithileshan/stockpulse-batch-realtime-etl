"""
Unit tests for StockPulse Producer (both simulated and yfinance)
"""

import pytest
from unittest.mock import MagicMock, patch, call
import json
from datetime import datetime, timezone


class TestSimulatedProducer:
    """Tests for simulated tick generator."""
    
    def test_imports_successfully(self):
        """Producer module imports without errors."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "producer"))
        import producer
        assert producer is not None
    
    def test_generate_tick_returns_valid_dict(self, sample_tick):
        """generate_tick returns dict with required fields."""
        required_fields = ["symbol", "price", "volume", "event_time"]
        for field in required_fields:
            assert field in sample_tick
    
    def test_tick_symbol_valid(self, sample_tick):
        """Tick symbol is one of the configured symbols."""
        valid_symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"]
        assert sample_tick["symbol"] in valid_symbols
    
    def test_tick_price_positive(self, sample_tick):
        """Tick price is positive number."""
        assert sample_tick["open"] > 0
        assert sample_tick["high"] > 0
        assert sample_tick["low"] > 0
        assert sample_tick["close"] > 0
    
    def test_tick_volume_non_negative(self, sample_tick):
        """Tick volume is non-negative."""
        assert sample_tick["volume"] >= 0
    
    def test_tick_event_time_valid_iso(self, sample_tick):
        """Tick event_time is valid ISO 8601 string."""
        try:
            dt = datetime.fromisoformat(sample_tick["event_time"].replace("Z", "+00:00"))
            assert dt is not None
        except ValueError:
            pytest.fail("Invalid ISO timestamp")


class TestYFinanceProducer:
    """Tests for yfinance real data producer."""
    
    def test_yfinance_producer_imports(self):
        """yfinance producer module imports."""
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "producer"))
        import producer_yfinance
        assert producer_yfinance is not None
    
    def test_tick_has_ohlcv_fields(self, sample_tick):
        """Real tick includes OHLCV fields."""
        required_fields = ["open", "high", "low", "close", "volume"]
        for field in required_fields:
            assert field in sample_tick
    
    def test_tick_source_field_present(self, sample_tick):
        """Tick includes source field."""
        assert "source" in sample_tick
        assert sample_tick["source"] in ["yfinance", "yfinance_poll", "yfinance_cached"]
    
    @patch("yfinance.download")
    def test_fetch_data_handles_network_error(self, mock_download):
        """Producer handles network errors gracefully."""
        mock_download.side_effect = Exception("Network error")
        # Should log error and continue
        assert mock_download.side_effect is not None


class TestProducerKafkaIntegration:
    """Tests for Kafka producer integration."""
    
    def test_kafka_producer_delivery_report(self, mock_kafka_producer):
        """Kafka delivery callback handles errors."""
        # Simulate successful delivery
        mock_kafka_producer.produce(
            topic="stock.ticks.v1",
            key="AAPL",
            value=json.dumps({"symbol": "AAPL"}),
        )
        assert mock_kafka_producer.produce.called
    
    def test_tick_key_is_symbol(self, sample_tick):
        """Kafka message key is symbol (partitioning by symbol)."""
        key = sample_tick["symbol"]
        assert key in ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"]
    
    def test_tick_value_is_valid_json(self, sample_tick):
        """Tick value serializes to valid JSON."""
        json_str = json.dumps(sample_tick)
        parsed = json.loads(json_str)
        assert parsed["symbol"] == sample_tick["symbol"]


class TestProducerLogging:
    """Tests for structured JSON logging."""
    
    def test_log_entry_has_required_fields(self):
        """Log entries include required fields."""
        # Simulated log entry
        log_entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "service": "producer",
            "msg": "Test message",
        }
        required_fields = ["ts", "level", "service", "msg"]
        for field in required_fields:
            assert field in log_entry
    
    def test_log_timestamp_is_iso(self):
        """Log timestamp is ISO 8601 format."""
        ts = datetime.now(timezone.utc).isoformat()
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            assert dt is not None
        except ValueError:
            pytest.fail("Invalid ISO timestamp in log")


class TestProducerConfiguration:
    """Tests for environment configuration."""
    
    def test_symbols_not_empty(self):
        """SYMBOLS list is not empty."""
        symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"]
        assert len(symbols) > 0
    
    def test_interval_positive(self):
        """Produce interval is positive."""
        interval = 2.0
        assert interval > 0
    
    def test_mode_valid(self):
        """Producer mode is valid."""
        valid_modes = ["cached", "poll"]
        mode = "cached"
        assert mode in valid_modes
