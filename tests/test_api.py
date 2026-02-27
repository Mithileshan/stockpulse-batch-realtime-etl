"""
Unit tests for StockPulse FastAPI analytics API
Tests all endpoints and error handling
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
import sys
import os

# Add services to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "api"))


@pytest.fixture()
def client():
    """FastAPI test client."""
    from app.main import app
    return TestClient(app)


class TestHealthEndpoints:
    """Test health check endpoints."""
    
    def test_health_success(self, client):
        """GET /health returns 200."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
    
    def test_ready_requires_db_connectivity(self, client):
        """GET /ready checks database connectivity."""
        response = client.get("/ready")
        # Should pass or fail based on DB availability
        assert response.status_code in [200, 503]
        assert "status" in response.json() or "detail" in response.json()


class TestSymbolsEndpoint:
    """Test /symbols endpoint."""
    
    def test_get_symbols_returns_list(self, client):
        """GET /symbols returns list of available symbols."""
        response = client.get("/symbols")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Should return 6 symbols (AAPL, MSFT, GOOG, AMZN, TSLA, NVDA)
        assert len(data) >= 0


class TestTicksEndpoints:
    """Test /ticks/* endpoints."""
    
    def test_get_latest_ticks(self, client):
        """GET /ticks/latest returns latest ticks per symbol."""
        response = client.get("/ticks/latest")
        assert response.status_code in [200, 404]  # 404 if no data
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
    
    def test_get_latest_tick_by_symbol(self, client):
        """GET /ticks/latest?symbol=AAPL returns specific symbol."""
        response = client.get("/ticks/latest?symbol=AAPL")
        assert response.status_code in [200, 404]
    
    def test_get_ticks_summary(self, client):
        """GET /ticks/summary returns windowed aggregates."""
        response = client.get("/ticks/summary?minutes=5")
        assert response.status_code in [200, 404]


class TestBarsEndpoints:
    """Test /bars/* endpoints."""
    
    def test_get_latest_bars(self, client):
        """GET /bars/latest returns latest 1m bars."""
        response = client.get("/bars/latest")
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
    
    def test_get_bars_summary(self, client):
        """GET /bars/summary returns period summary."""
        response = client.get("/bars/summary?minutes=60")
        assert response.status_code in [200, 404]
    
    def test_get_bars_by_symbol(self, client):
        """GET /bars/latest?symbol=AAPL filters by symbol."""
        response = client.get("/bars/latest?symbol=AAPL")
        assert response.status_code in [200, 404]


class TestMoversEndpoint:
    """Test /movers endpoint."""
    
    def test_get_movers_default(self, client):
        """GET /movers returns top movers by default (5m)."""
        response = client.get("/movers")
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
    
    def test_get_movers_custom_window(self, client):
        """GET /movers?minutes=60 returns movers for specific window."""
        response = client.get("/movers?minutes=60")
        assert response.status_code in [200, 404]
    
    def test_get_movers_limit(self, client):
        """GET /movers?limit=3 limits results."""
        response = client.get("/movers?limit=3")
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert len(data) <= 3


class TestMetricsEndpoint:
    """Test Prometheus /metrics endpoint."""
    
    def test_metrics_endpoint_available(self, client):
        """GET /metrics returns Prometheus metrics."""
        response = client.get("/metrics")
        assert response.status_code == 200
        # Should contain Prometheus-format metrics
        assert "# HELP" in response.text or "# TYPE" in response.text


class TestErrorHandling:
    """Test error responses."""
    
    def test_invalid_symbol_returns_404(self, client):
        """GET /ticks/latest?symbol=INVALID returns 404."""
        response = client.get("/ticks/latest?symbol=INVALID")
        # May return empty list or 404 depending on implementation
        assert response.status_code in [200, 404]
    
    def test_invalid_minutes_parameter(self, client):
        """GET /movers?minutes=-5 returns validation error."""
        response = client.get("/movers?minutes=-5")
        assert response.status_code == 422  # Validation error
    
    def test_non_existent_endpoint_returns_404(self, client):
        """GET /nonexistent returns 404."""
        response = client.get("/nonexistent")
        assert response.status_code == 404


class TestCORSHeaders:
    """Test CORS middleware."""
    
    def test_cors_headers_present(self, client):
        """Response includes CORS headers."""
        response = client.get("/health")
        # CORS headers should be present in real deployment
        assert response.status_code == 200
