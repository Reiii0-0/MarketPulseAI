from __future__ import annotations

import contextlib
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_db, get_predictor
from api.main import app


@pytest.fixture
def test_app(loaded_predictor, mock_db_connection):
    """Override dependencies and return TestClient."""
    db_mgr_mock = MagicMock()
    db_mgr_mock.get_connection.return_value.__enter__.return_value = mock_db_connection

    app.dependency_overrides[get_predictor] = lambda: loaded_predictor
    app.dependency_overrides[get_db] = lambda: db_mgr_mock

    # Completely override the lifespan to bypass DB init
    @contextlib.asynccontextmanager
    async def mock_lifespan(app_inst):
        app_inst.state.model_loaded = True
        app_inst.state.db_manager = db_mgr_mock
        app_inst.state.predictor = loaded_predictor
        import time

        app_inst.state.start_time = time.time() - 3600
        yield

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = mock_lifespan

    with TestClient(app) as client:
        yield client

    app.router.lifespan_context = original_lifespan
    app.dependency_overrides.clear()


@pytest.mark.integration
def test_health_endpoint_returns_200_when_healthy(test_app):
    """T-069a: Verify /health returns 200 and healthy status."""
    response = test_app.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
    assert response.json()["model_loaded"] is True


@pytest.mark.integration
def test_predict_endpoint_returns_200_for_valid_request(test_app, fake_market_features):
    """T-069b: Verify /predict accepts valid payload and returns PredictionResponse."""
    response = test_app.post("/api/v1/predict", json=fake_market_features)
    assert response.status_code == 200
    data = response.json()
    assert data["ticker"] == "AAPL"
    assert "predicted_direction" in data
    assert len(data["factors"]) == 3


@pytest.mark.integration
def test_predict_endpoint_returns_422_for_invalid_rsi(test_app, fake_market_features):
    """T-069c: Verify Pydantic constraint on RSI > 100 is caught."""
    fake_market_features["rsi_14"] = 150.0
    response = test_app.post("/api/v1/predict", json=fake_market_features)
    assert response.status_code == 422
    assert "VALIDATION_ERROR" in response.text


@pytest.mark.integration
def test_predict_endpoint_returns_422_for_high_less_than_low(test_app, fake_market_features):
    """T-069d: Verify cross-field validation for high < low."""
    fake_market_features["high"] = 100.0
    fake_market_features["low"] = 110.0
    response = test_app.post("/api/v1/predict", json=fake_market_features)
    assert response.status_code == 422
    assert "high price cannot be less than low price" in response.text


@pytest.mark.integration
def test_predict_endpoint_response_contains_three_shap_factors(test_app, fake_market_features):
    """T-069e: Verify SHAP factor count in API response."""
    response = test_app.post("/api/v1/predict", json=fake_market_features)
    assert response.status_code == 200
    assert len(response.json()["factors"]) == 3


@pytest.mark.integration
def test_batch_predict_endpoint_returns_correct_count(test_app, fake_market_features):
    """T-069f: Verify /predict/batch processes multiple requests."""
    payload = {"requests": [fake_market_features] * 5}
    response = test_app.post("/api/v1/predict/batch", json=payload)
    assert response.status_code == 200
    assert response.json()["count"] == 5


@pytest.mark.integration
def test_batch_predict_rejects_empty_requests_list(test_app):
    """T-069g: Verify batch endpoint requires at least 1 request."""
    response = test_app.post("/api/v1/predict/batch", json={"requests": []})
    assert response.status_code == 422


@pytest.mark.integration
def test_model_info_endpoint_returns_feature_names(test_app):
    """T-069h: Verify /model/info returns metadata feature list."""
    response = test_app.get("/api/v1/model/info")
    assert response.status_code == 200
    assert len(response.json()["feature_names"]) == 15
