from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.dependencies import get_db, get_predictor
from api.routers.health import router

def create_test_app(mock_db=None, mock_predictor=None):
    app = FastAPI()
    app.include_router(router)
    if mock_db:
        app.dependency_overrides[get_db] = lambda: mock_db
    if mock_predictor:
        app.dependency_overrides[get_predictor] = lambda: mock_predictor
    return app

def test_health_db_fail():
    """Verifies health endpoint when DB is unreachable."""
    mock_db = MagicMock()
    mock_db.get_connection.side_effect = Exception("DB Down")
    
    client = TestClient(create_test_app(mock_db=mock_db))
    response = client.get("/health")
    
    assert response.status_code == 200
    assert response.json()["status"] == "degraded"

def test_health_predictor_fail():
    """Verifies health endpoint when Predictor is missing."""
    client = TestClient(create_test_app(mock_predictor=None)) # Predictor None in state
    response = client.get("/health")
    # Note: our router checks if predictor is None
    assert response.status_code == 200
    assert response.json()["model_loaded"] is False
