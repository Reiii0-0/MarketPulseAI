from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.dependencies import get_predictor
from api.routers.model import router

def create_test_app(mock_predictor):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_predictor] = lambda: mock_predictor
    return app

def test_get_model_info_no_predictor():
    """Verifies model info 503 when predictor is not loaded."""
    client = TestClient(create_test_app(None))
    response = client.get("/model/info")
    assert response.status_code == 503
