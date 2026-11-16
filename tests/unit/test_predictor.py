from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pytest

from model.predictor import Predictor


@pytest.fixture
def mock_metadata():
    return {
        "model_version": "xgb_v1.0.0",
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


@pytest.fixture
def predictor(mock_xgb_model, mock_metadata, mock_shap_explainer):
    scaler = MagicMock()
    # Mock scaler transform to return same shape as input
    scaler.transform.side_effect = lambda x: x.values

    return Predictor(
        model=mock_xgb_model, scaler=scaler, metadata=mock_metadata, explainer=mock_shap_explainer
    )


@pytest.mark.unit
def test_predictor_returns_up_when_probability_above_half(
    predictor, fake_market_features, mock_xgb_model
):
    """T-053a: Verify predicted_direction is 1 for high prob_up."""
    mock_xgb_model.get_booster().predict.return_value = np.array([0.8])

    result = predictor.predict(fake_market_features)
    assert result["predicted_direction"] == 1
    assert result["probability_up"] == 0.8


@pytest.mark.unit
def test_predictor_returns_down_when_probability_below_half(
    predictor, fake_market_features, mock_xgb_model
):
    """T-053b: Verify predicted_direction is 0 for high prob_down."""
    mock_xgb_model.get_booster().predict.return_value = np.array([0.3])

    result = predictor.predict(fake_market_features)
    assert result["predicted_direction"] == 0
    assert result["probability_down"] == 0.7


@pytest.mark.unit
def test_predictor_probability_up_plus_down_equals_one(predictor, fake_market_features):
    """T-053c: Verify probabilities sum to 1."""
    result = predictor.predict(fake_market_features)
    assert result["probability_up"] + result["probability_down"] == pytest.approx(1.0)


@pytest.mark.unit
def test_predictor_confidence_tier_high_above_075(predictor, fake_market_features, mock_xgb_model):
    """T-053d: Verify HIGH confidence tier."""
    mock_xgb_model.get_booster().predict.return_value = np.array([0.9])
    result = predictor.predict(fake_market_features)
    assert result["confidence_tier"] == "HIGH"


@pytest.mark.unit
def test_predictor_confidence_tier_medium_between_060_and_075(
    predictor, fake_market_features, mock_xgb_model
):
    """T-053e: Verify MEDIUM confidence tier."""
    mock_xgb_model.get_booster().predict.return_value = np.array([0.65])
    result = predictor.predict(fake_market_features)
    assert result["confidence_tier"] == "MEDIUM"


@pytest.mark.unit
def test_predictor_confidence_tier_low_at_or_below_060(
    predictor, fake_market_features, mock_xgb_model
):
    """T-053f: Verify LOW confidence tier."""
    mock_xgb_model.get_booster().predict.return_value = np.array([0.55])
    result = predictor.predict(fake_market_features)
    assert result["confidence_tier"] == "LOW"


@pytest.mark.unit
def test_predictor_returns_exactly_three_shap_factors(predictor, fake_market_features):
    """T-053g: Verify SHAP factor count."""
    result = predictor.predict(fake_market_features)
    assert len(result["factors"]) == 3


@pytest.mark.unit
def test_predictor_shap_factors_are_sorted_by_abs_value_descending(
    predictor, fake_market_features, mock_shap_explainer
):
    """T-053h: Verify SHAP sorting."""
    # Mock SHAP values with specific magnitudes
    vals = np.zeros(15)
    vals[0] = 0.5  # open
    vals[1] = -0.8  # high
    vals[2] = 0.2  # low

    # Wrap in an object that has a .values attribute
    mock_shap_values = MagicMock()
    mock_shap_values.values = np.array([vals])
    mock_shap_explainer.return_value = mock_shap_values

    result = predictor.predict(fake_market_features)
    factors = result["factors"]

    assert factors[0]["feature"] == "high"
    assert factors[1]["feature"] == "open"
    assert factors[2]["feature"] == "low"
    assert abs(factors[0]["shap_value"]) > abs(factors[1]["shap_value"])
