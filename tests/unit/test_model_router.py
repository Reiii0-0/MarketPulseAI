from __future__ import annotations

from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.dependencies import get_predictor
from api.routers.model import router


def create_test_app(mock_predictor):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_predictor] = lambda: mock_predictor
    return app


def test_get_model_info_success():
    """Verifies successful model info retrieval."""
    mock_predictor = MagicMock()
    mock_predictor.metadata = {
        "model_version": "xgb_v1.0.0",
        "training_date": "2023-01-01T12:00:00",
        "feature_names": ["f1", "f2"],
        "accuracy": 0.95,
    }

    client = TestClient(create_test_app(mock_predictor))
    response = client.get("/model/info")

    assert response.status_code == 200
    data = response.json()
    assert data["model_version"] == "xgb_v1.0.0"
    assert data["feature_count"] == 2
