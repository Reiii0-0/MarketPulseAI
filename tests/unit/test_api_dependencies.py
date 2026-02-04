from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from api.dependencies import get_db, get_predictor

def test_get_db_dependency():
    """Verifies get_db dependency returns db_manager from app state."""
    mock_request = MagicMock()
    mock_request.app.state.db_manager = "mock_db"
    assert get_db(mock_request) == "mock_db"

def test_get_predictor_dependency():
    """Verifies get_predictor dependency returns predictor from app state."""
    mock_request = MagicMock()
    mock_request.app.state.predictor = "mock_predictor"
    assert get_predictor(mock_request) == "mock_predictor"
