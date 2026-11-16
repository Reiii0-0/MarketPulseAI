from __future__ import annotations

from fastapi.testclient import TestClient

from api.main import app


def test_health_detailed():
    """Verifies health endpoint detail logic."""
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    # Trigger the model_loaded path
    assert "model_loaded" in data
