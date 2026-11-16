from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.dependencies import get_db, get_predictor
from api.routers.predict import router


def create_test_app(mock_predictor, mock_db):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_predictor] = lambda: mock_predictor
    app.dependency_overrides[get_db] = lambda: mock_db
    return app


@pytest.fixture
def mock_predictor():
    return MagicMock()


@pytest.fixture
def mock_db():
    return MagicMock()


def test_predict_success(mock_predictor, mock_db):
    """Verifies successful single prediction endpoint."""
    mock_predictor.predict.return_value = {
        "ticker": "AAPL",
        "window_start": "2023-01-01T00:00:00",
        "predicted_direction": 1,
        "probability_up": 0.8,
        "probability_down": 0.2,
        "confidence_tier": "HIGH",
        "factors": [],
        "model_version": "v1",
    }

    client = TestClient(create_test_app(mock_predictor, mock_db))
    payload = {
        "ticker": "AAPL",
        "window_start": "2023-01-01T00:00:00Z",
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 102.0,
        "volume": 1000,
    }
    response = client.post("/predict", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["predicted_direction"] == "UP"
    assert data["probability_up"] == 0.8
    assert mock_db.execute_upsert.called


def test_predict_error_handling(mock_predictor, mock_db):
    """Verifies 500 error when predictor fails."""
    mock_predictor.predict.side_effect = Exception("Model error")

    client = TestClient(create_test_app(mock_predictor, mock_db))
    payload = {
        "ticker": "AAPL",
        "window_start": "2023-01-01T00:00:00Z",
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 102.0,
        "volume": 1000,
    }
    response = client.post("/predict", json=payload)

    assert response.status_code == 500
    assert response.json()["detail"]["error"] == "INTERNAL_SERVER_ERROR"
    assert "Model error" not in response.text


def test_get_recent_predictions_empty(mock_db):
    """Verifies recent predictions endpoint with empty results."""
    mock_conn = mock_db.get_connection.return_value.__enter__.return_value
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value
    mock_cur.fetchone.return_value = [0]  # Total count
    mock_cur.fetchall.return_value = []  # Rows

    client = TestClient(create_test_app(MagicMock(), mock_db))
    response = client.get("/predictions/recent")

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["predictions"] == []


def test_predict_batch_success(mock_predictor, mock_db):
    """Verifies successful batch prediction endpoint."""
    mock_predictor.predict.return_value = {
        "ticker": "AAPL",
        "window_start": "2023-01-01T00:00:00",
        "predicted_direction": 1,
        "probability_up": 0.8,
        "probability_down": 0.2,
        "confidence_tier": "HIGH",
        "factors": [],
        "model_version": "v1",
    }

    client = TestClient(create_test_app(mock_predictor, mock_db))
    payload = {
        "requests": [
            {
                "ticker": "AAPL",
                "window_start": "2023-01-01T00:00:00Z",
                "open": 100.0,
                "high": 105.0,
                "low": 95.0,
                "close": 102.0,
                "volume": 1000,
            },
            {
                "ticker": "AAPL",
                "window_start": "2023-01-01T00:05:00Z",
                "open": 102.0,
                "high": 107.0,
                "low": 101.0,
                "close": 104.0,
                "volume": 1200,
            },
        ]
    }
    response = client.post("/predict/batch", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2
    assert len(data["predictions"]) == 2
    assert mock_db.execute_upsert.call_count == 2


def test_get_recent_predictions_with_data(mock_db):
    """Verifies recent predictions endpoint with mock data."""
    mock_conn = mock_db.get_connection.return_value.__enter__.return_value
    mock_cur = mock_conn.cursor.return_value.__enter__.return_value
    mock_cur.fetchone.return_value = [1]
    mock_cur.fetchall.return_value = [
        [
            str(uuid.uuid4()),
            "AAPL",
            "2023-01-01T00:00:00",
            1,
            0.8,
            "HIGH",
            1,
            True,
            "2023-01-01T00:00:05",
        ]
    ]

    client = TestClient(create_test_app(MagicMock(), mock_db))
    response = client.get("/predictions/recent", params={"ticker": "AAPL", "limit": 10})

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["predictions"][0]["ticker"] == "AAPL"
    assert data["predictions"][0]["predicted_direction"] == "UP"
