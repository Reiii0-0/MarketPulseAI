from __future__ import annotations

from fastapi import Request

from consumer.db import DatabaseManager
from model.predictor import Predictor


from typing import cast

def get_db(request: Request) -> DatabaseManager:
    """Dependency to retrieve the database manager from app state."""
    return cast(DatabaseManager, request.app.state.db_manager)

def get_predictor(request: Request) -> Predictor:
    """Dependency to retrieve the loaded XGBoost predictor from app state."""
    return cast(Predictor, request.app.state.predictor)
