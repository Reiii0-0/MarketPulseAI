from __future__ import annotations

import os

import numpy as np
import pytest

os.environ.setdefault("POSTGRES_DB", "marketpulse")
os.environ.setdefault("POSTGRES_USER", "marketpulse_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")


@pytest.fixture
def fake_market_tick():
    """Valid market tick data matching SCHEMA.md §2.1."""
    return {
        "ticker": "AAPL",
        "event_timestamp": "2024-01-15T09:35:00.000000Z",
        "price": 185.2350,
        "bid": 185.2300,
        "ask": 185.2400,
        "volume": 12500,
        "trade_condition": "regular",
        "replay_sequence": 100042,
    }


@pytest.fixture
def fake_sentiment_event():
    """Valid sentiment event data matching SCHEMA.md §2.2."""
    return {
        "ticker": "AAPL",
        "event_timestamp": "2024-01-15T09:33:47.000000Z",
        "source": "reuters",
        "headline": "Apple beats Q4 revenue estimates",
        "sentiment_score": 0.7821,
        "sentiment_label": "POSITIVE",
        "confidence": 0.9234,
        "replay_sequence": 5001,
    }


@pytest.fixture
def fake_market_features():
    """Valid prediction request data matching SCHEMA.md §1.3."""
    return {
        "ticker": "AAPL",
        "window_start": "2024-01-15T09:35:00Z",
        "open": 184.50,
        "high": 185.80,
        "low": 184.20,
        "close": 185.23,
        "volume": 1250000,
        "sma_20": 183.41,
        "ema_12": 184.72,
        "ema_26": 183.95,
        "rsi_14": 61.34,
        "macd_line": 0.7700,
        "macd_signal": 0.5820,
        "macd_histogram": 0.1880,
        "avg_sentiment_score": 0.3450,
        "sentiment_event_count": 3,
        "sentiment_polarity_shift": 0.0812,
    }


@pytest.fixture
def mock_xgb_model(mocker):
    """Mock XGBoost classifier for inference tests."""
    model = mocker.MagicMock()
    model.predict_proba.return_value = np.array([[0.2766, 0.7234]])
    model.get_booster.return_value.predict.return_value = np.array([0.7234])
    model.feature_names_in_ = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "sma_20",
        "ema_12",
        "ema_26",
        "rsi_14",
        "macd_line",
        "macd_signal",
        "macd_histogram",
        "avg_sentiment_score",
        "sentiment_event_count",
        "sentiment_polarity_shift",
    ]
    return model


@pytest.fixture
def mock_kafka_producer(mocker):
    """Mock Kafka producer for testing publish logic."""
    producer = mocker.MagicMock()
    return producer


@pytest.fixture
def mock_db_connection(mocker):
    """Mock psycopg2 connection for testing database operations."""
    conn = mocker.MagicMock()
    return conn


@pytest.fixture
def mock_shap_explainer(mocker):
    explainer = mocker.MagicMock()

    # Create specific SHAP values array for factors
    vals = np.zeros(15)
    vals[0] = 0.5  # open
    vals[1] = -0.8  # high
    vals[2] = 0.2  # low

    mock_shap_values = mocker.MagicMock()
    mock_shap_values.values = np.array([vals])
    explainer.return_value = mock_shap_values
    return explainer


@pytest.fixture
def loaded_predictor(mock_xgb_model, mock_shap_explainer, mocker):
    from model.predictor import Predictor

    scaler = mocker.MagicMock()
    scaler.transform.side_effect = lambda x: x.values

    metadata = {
        "model_version": "xgb_v1.0.0",
        "training_date": "2024-01-10T00:00:00Z",
        "accuracy": 0.78,
        "feature_names": [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "sma_20",
            "ema_12",
            "ema_26",
            "rsi_14",
            "macd_line",
            "macd_signal",
            "macd_histogram",
            "avg_sentiment_score",
            "sentiment_event_count",
            "sentiment_polarity_shift",
        ],
    }

    return Predictor(
        model=mock_xgb_model, scaler=scaler, metadata=metadata, explainer=mock_shap_explainer
    )
