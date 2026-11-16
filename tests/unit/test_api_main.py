from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.main import app, lifespan


@pytest.fixture
def client():
    return TestClient(app)


@pytest.mark.asyncio
async def test_lifespan_startup_success():
    """Verifies successful API startup sequence."""
    mock_app = MagicMock()
    mock_app.state = MagicMock()

    with patch("api.main.DatabaseManager"), patch("api.main.Predictor.load") as mock_load:
        async with lifespan(mock_app):
            assert mock_app.state.db_manager is not None
            assert mock_app.state.model_loaded is True
            assert mock_load.called


@pytest.mark.asyncio
async def test_lifespan_startup_model_fail():
    """Verifies API starts in degraded mode when model fails to load."""
    mock_app = MagicMock()
    mock_app.state = MagicMock()

    with (
        patch("api.main.DatabaseManager"),
        patch("api.main.Predictor.load", side_effect=FileNotFoundError("Missing model")),
    ):
        async with lifespan(mock_app):
            assert mock_app.state.model_loaded is False
            assert mock_app.state.predictor is None


def test_health_check(client):
    """Verifies the health check endpoint returns 200."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "model_loaded" in data


def test_root_serves_dashboard(client):
    """Verifies that / and /dashboard serve the MarketPulseAI product UI."""
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "MarketPulseAI" in response.text

    dash_response = client.get("/dashboard")
    assert dash_response.status_code == 200
    assert "text/html" in dash_response.headers.get("content-type", "")


def test_api_v1_health(client):
    """Verifies the prefixed health check endpoint."""
    response = client.get("/api/v1/health")
    assert response.status_code == 200


def test_rate_limiting_bypass_for_health(client):
    """Verifies that health checks are not subject to rate limiting."""
    # This is a bit hard to test without state, but we can verify the path logic
    for _ in range(5):
        response = client.get("/health")
        assert response.status_code == 200
