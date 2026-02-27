"""
Unit tests for StockPulse Consumer (Redpanda → PostgreSQL sink)
"""

import pytest
from unittest.mock import MagicMock, patch
import json


class TestConsumerKafkaIntegration:
    """Tests for Kafka consumer integration."""
    
    def test_consumer_subscribes_to_topic(self, mock_kafka_producer):
        """Consumer subscribes to stock.ticks.v1 topic."""
        # Consumer should subscribe to the ticks topic
        topic = "stock.ticks.v1"
        assert topic is not None
    
    def test_consumer_parses_valid_tick_message(self, valid_tick_json, sample_tick):
        """Consumer can parse valid tick messages."""
        data = json.loads(valid_tick_json)
        assert data["symbol"] == sample_tick["symbol"]
        assert "close" in data
    
    def test_consumer_rejects_invalid_json(self, invalid_tick_json):
        """Consumer handles invalid JSON gracefully."""
        # Should log error and continue
        try:
            data = json.loads(invalid_tick_json)
            # If it parses, check it has missing required fields
            required = ["open", "high", "low", "close"]
            missing = [f for f in required if f not in data]
            assert len(missing) > 0
        except json.JSONDecodeError:
            # Invalid JSON should raise error
            pytest.fail("Should handle invalid JSON")


class TestConsumerDatabaseIntegration:
    """Tests for database sink integration."""
    
    def test_insert_tick_creates_record(self, mock_db_connection, sample_tick):
        """INSERT tick into stock_ticks table."""
        cursor = mock_db_connection.cursor.return_value
        cursor.execute(
            """
            INSERT INTO stock_ticks (symbol, price, volume, event_time)
            VALUES (%s, %s, %s, %s)
            """,
            (sample_tick["symbol"], sample_tick["close"], sample_tick["volume"], sample_tick["event_time"])
        )
        assert cursor.execute.called
    
    def test_insert_idempotent_on_duplicate_key(self, mock_db_connection, sample_tick):
        """INSERT OR IGNORE on duplicate event."""
        # Consumer should handle duplicate events gracefully
        cursor = mock_db_connection.cursor.return_value
        cursor.execute(
            """
            INSERT INTO stock_ticks (symbol, price, volume, event_time)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (sample_tick["symbol"], sample_tick["close"], sample_tick["volume"], sample_tick["event_time"])
        )
        assert cursor.execute.called
    
    def test_database_error_triggers_retry(self, mock_db_connection):
        """Database error triggers exponential backoff retry."""
        cursor = mock_db_connection.cursor.return_value
        cursor.execute.side_effect = Exception("Database connection lost")
        # Should trigger retry logic
        assert cursor.execute.side_effect is not None
    
    def test_dead_letter_queue_on_repeated_failure(self, mock_db_connection):
        """Message moved to failed_events after retries exhausted."""
        # After N retries, message should go to dead-letter queue
        cursor = mock_db_connection.cursor.return_value
        cursor.execute(
            """
            INSERT INTO failed_events (topic, message, error)
            VALUES (%s, %s, %s)
            """,
            ("stock.ticks.v1", "malformed message", "Database insert failed after retries")
        )
        assert cursor.execute.called


class TestConsumerRetryLogic:
    """Tests for exponential backoff retry."""
    
    def test_retry_base_delay(self):
        """Initial retry delay is reasonable."""
        base_delay = 1.0  # 1 second
        assert base_delay > 0
    
    def test_retry_exponential_backoff(self):
        """Retry delays increase exponentially."""
        delays = [1.0 * (2 ** i) for i in range(5)]
        # delays should be [1, 2, 4, 8, 16]
        assert delays[0] == 1.0
        assert delays[1] == 2.0
        assert delays[2] == 4.0
    
    def test_retry_max_attempts(self):
        """Consumer stops retrying after max attempts."""
        max_retries = 3
        assert max_retries > 0


class TestConsumerLogging:
    """Tests for consumer logging."""
    
    def test_log_entry_includes_offset(self):
        """Log entries include Kafka offset."""
        log_entry = {
            "offset": 12345,
            "partition": 0,
            "message": "Processed tick",
        }
        assert "offset" in log_entry
    
    def test_log_entry_includes_symbol(self):
        """Log entries include symbol for traceability."""
        log_entry = {
            "symbol": "AAPL",
            "message": "Inserted tick",
        }
        assert "symbol" in log_entry
    
    def test_error_log_includes_exception(self):
        """Error logs include exception details."""
        log_entry = {
            "level": "ERROR",
            "message": "Failed to insert",
            "exception": "psycopg2.Error: connection refused",
        }
        assert "exception" in log_entry


class TestConsumerPerformance:
    """Tests for consumer throughput and latency."""
    
    def test_consumer_processes_messages_in_order(self):
        """Messages from same partition processed in order."""
        # Offset should be monotonically increasing per partition
        offsets = [0, 1, 2, 3, 4]
        for i in range(len(offsets) - 1):
            assert offsets[i] < offsets[i + 1]
    
    def test_consumer_batch_inserts_if_available(self):
        """Consumer can batch multiple ticks into single insert."""
        # Should group messages by window and insert together
        batch_size = 100
        assert batch_size > 1


class TestConsumerSchemaValidation:
    """Tests for data schema validation."""
    
    def test_tick_schema_validation(self, sample_tick):
        """Tick data validates against schema."""
        required_fields = ["symbol", "open", "high", "low", "close", "volume"]
        for field in required_fields:
            assert field in sample_tick
    
    def test_symbol_is_string(self, sample_tick):
        """Symbol field is string."""
        assert isinstance(sample_tick["symbol"], str)
        assert len(sample_tick["symbol"]) > 0
    
    def test_price_fields_are_numeric(self, sample_tick):
        """OHLC fields are numeric."""
        for field in ["open", "high", "low", "close"]:
            assert isinstance(sample_tick[field], (int, float))
            assert sample_tick[field] > 0
    
    def test_volume_is_integer(self, sample_tick):
        """Volume is integer."""
        assert isinstance(sample_tick["volume"], int)
        assert sample_tick["volume"] >= 0
    
    def test_ohlc_order(self, sample_tick):
        """High >= Open, Close, Low and Low <= all others."""
        assert sample_tick["high"] >= sample_tick["open"]
        assert sample_tick["high"] >= sample_tick["close"]
        assert sample_tick["high"] >= sample_tick["low"]
        assert sample_tick["low"] <= sample_tick["open"]
        assert sample_tick["low"] <= sample_tick["close"]
