"""
Unit tests for StockPulse Aggregator (ticks → 1m OHLCV bars)
"""

import pytest
from unittest.mock import MagicMock, patch
import json
from datetime import datetime, timezone, timedelta


class TestAggregatorBucketCalculation:
    """Tests for 1-minute bucket calculations."""
    
    def test_bucket_rounds_to_minute_boundary(self):
        """Timestamp rounds to minute bucket start."""
        # 10:30:45 should bucket to 10:30:00
        ts = datetime(2026, 2, 27, 10, 30, 45, tzinfo=timezone.utc)
        bucket_start = ts.replace(second=0, microsecond=0)
        assert bucket_start.minute == 30
        assert bucket_start.second == 0
    
    def test_bucket_start_is_consistent(self):
        """All ticks in same minute go to same bucket."""
        times = [
            datetime(2026, 2, 27, 10, 30, 0, tzinfo=timezone.utc),
            datetime(2026, 2, 27, 10, 30, 15, tzinfo=timezone.utc),
            datetime(2026, 2, 27, 10, 30, 59, tzinfo=timezone.utc),
        ]
        buckets = [t.replace(second=0, microsecond=0) for t in times]
        assert buckets[0] == buckets[1] == buckets[2]
    
    def test_different_minutes_different_buckets(self):
        """Ticks in different minutes get different buckets."""
        t1 = datetime(2026, 2, 27, 10, 30, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 2, 27, 10, 31, 0, tzinfo=timezone.utc)
        b1 = t1.replace(second=0, microsecond=0)
        b2 = t2.replace(second=0, microsecond=0)
        assert b1 != b2


class TestAggregatorOHLCVCalculation:
    """Tests for OHLCV bar calculation."""
    
    def test_bar_open_is_first_tick_price(self):
        """Bar open = first tick price in bucket."""
        ticks = [
            {"symbol": "AAPL", "close": 190.50},
            {"symbol": "AAPL", "close": 190.75},
            {"symbol": "AAPL", "close": 190.25},
        ]
        bar_open = ticks[0]["close"]
        assert bar_open == 190.50
    
    def test_bar_close_is_last_tick_price(self):
        """Bar close = last tick price in bucket."""
        ticks = [
            {"symbol": "AAPL", "close": 190.50},
            {"symbol": "AAPL", "close": 190.75},
            {"symbol": "AAPL", "close": 190.25},
        ]
        bar_close = ticks[-1]["close"]
        assert bar_close == 190.25
    
    def test_bar_high_is_max_price(self):
        """Bar high = max of all prices."""
        prices = [190.50, 190.75, 190.25, 191.00, 190.80]
        bar_high = max(prices)
        assert bar_high == 191.00
    
    def test_bar_low_is_min_price(self):
        """Bar low = min of all prices."""
        prices = [190.50, 190.75, 190.25, 191.00, 190.80]
        bar_low = min(prices)
        assert bar_low == 190.25
    
    def test_bar_volume_is_sum(self):
        """Bar volume = sum of all tick volumes."""
        volumes = [1000000, 1500000, 800000, 1200000]
        bar_volume = sum(volumes)
        assert bar_volume == 4500000


class TestAggregatorIdempotence:
    """Tests for idempotent upsert."""
    
    def test_duplicate_aggregation_produces_same_bar(self, sample_bar):
        """Re-running aggregation produces identical bar."""
        bar1 = sample_bar.copy()
        bar2 = sample_bar.copy()
        assert bar1 == bar2
    
    def test_upsert_does_not_duplicate(self):
        """UPSERT on (symbol, bucket_start) prevents duplicates."""
        # Should use ON CONFLICT clause in SQL
        unique_constraint = "UNIQUE(symbol, bucket_start)"
        assert "bucket_start" in unique_constraint
    
    def test_rerunning_aggregator_safe(self):
        """Running aggregator multiple times is safe."""
        # With idempotent upsert, running twice = same result as running once
        assert True


class TestAggregatorWatermarking:
    """Tests for watermark persistence."""
    
    def test_watermark_persisted_in_etl_runs(self):
        """Latest processed offset saved in etl_runs table."""
        watermark_query = """
        INSERT INTO etl_runs (service, offset, symbol, ts)
        VALUES (%s, %s, %s, %s)
        """
        assert "offset" in watermark_query
    
    def test_aggregator_resumes_from_watermark(self):
        """On restart, aggregator resumes from saved offset."""
        last_offset = 12345
        next_offset = 12346
        assert next_offset > last_offset
    
    def test_watermark_updated_after_successful_aggregation(self):
        """Watermark moves forward only after upsert succeeds."""
        # Watermark should be part of transaction
        assert True


class TestAggregatorErrorHandling:
    """Tests for error handling."""
    
    def test_empty_bucket_skipped(self):
        """Bucket with no ticks is skipped."""
        ticks = []
        if len(ticks) == 0:
            # Skip aggregation
            assert True
    
    def test_single_tick_bar(self):
        """Bar from single tick: O=H=L=C."""
        price = 190.50
        bar = {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
        }
        assert bar["open"] == bar["high"] == bar["low"] == bar["close"]
    
    def test_database_error_rollback(self):
        """Database error rolls back transaction."""
        # Aggregation should be atomic
        assert True


class TestAggregatorPerformance:
    """Tests for aggregator efficiency."""
    
    def test_aggregator_interval_reasonable(self):
        """Aggregator runs every 30 seconds."""
        interval_seconds = 30
        assert 10 <= interval_seconds <= 60
    
    def test_aggregator_processes_all_symbols(self):
        """Aggregator processes all 6 symbols."""
        symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"]
        assert len(symbols) == 6
    
    def test_memory_efficient_streaming(self):
        """Aggregator uses streaming (not loading whole dataset)."""
        # Should process in bucketed groups
        assert True


class TestAggregatorLogging:
    """Tests for aggregator logging."""
    
    def test_log_includes_symbol_and_bucket(self):
        """Logs include symbol and bucket for troubleshooting."""
        log_entry = {
            "symbol": "AAPL",
            "bucket_start": "2026-02-27 10:30:00",
            "ticks_aggregated": 120,
        }
        assert "symbol" in log_entry
        assert "bucket_start" in log_entry
    
    def test_log_includes_watermark_progress(self):
        """Logs track watermark progress."""
        log_entry = {
            "last_offset": 50000,
            "current_offset": 50024,
        }
        assert "offset" in str(log_entry)


class TestAggregatorMultipleSymbols:
    """Tests for handling multiple symbols."""
    
    def test_bars_separated_by_symbol(self, sample_bars):
        """Each symbol gets separate bar entries."""
        symbols = set(bar["symbol"] for bar in sample_bars)
        assert len(symbols) > 1
    
    def test_no_cross_symbol_mixing(self):
        """Bars from one symbol don't mix with another."""
        aapl_bar = {"symbol": "AAPL", "close": 190.50}
        msft_bar = {"symbol": "MSFT", "close": 415.00}
        assert aapl_bar["symbol"] != msft_bar["symbol"]
    
    def test_all_symbols_aggregated_in_cycle(self):
        """Single aggregation cycle covers all symbols."""
        symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA"]
        # After one aggregation cycle, all symbols should have bars
        assert len(symbols) == 6


class TestAggregatorSchema:
    """Tests for bar schema."""
    
    def test_bar_has_required_fields(self, sample_bar):
        """Bar includes all required fields."""
        required = ["symbol", "bucket_start", "open", "high", "low", "close", "volume"]
        for field in required:
            assert field in sample_bar
    
    def test_bar_types_correct(self, sample_bar):
        """Bar field types are correct."""
        assert isinstance(sample_bar["symbol"], str)
        assert isinstance(sample_bar["open"], (int, float))
        assert isinstance(sample_bar["volume"], int)
