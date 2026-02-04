from __future__ import annotations

import pandas as pd
import pytest
from model.feature_engineering import calculate_indicators, calculate_sentiment_features

@pytest.fixture
def sample_ohlcv_data():
    """Provides a minimal OHLCV dataset for testing indicators."""
    data = {
        "ticker": ["AAPL"] * 30,
        "window_start": pd.date_range("2023-01-01", periods=30, freq="5min"),
        "open": [100.0 + i for i in range(30)],
        "high": [101.0 + i for i in range(30)],
        "low": [99.0 + i for i in range(30)],
        "close": [100.5 + i for i in range(30)],
        "volume": [1000 + i * 10 for i in range(30)]
    }
    return pd.DataFrame(data)

def test_calculate_indicators_output_columns(sample_ohlcv_data):
    """Verifies that all expected indicator columns are present."""
    result = calculate_indicators(sample_ohlcv_data)
    
    expected_cols = [
        "sma_20", "ema_12", "ema_26", 
        "macd_line", "macd_signal", "macd_histogram", 
        "rsi_14"
    ]
    for col in expected_cols:
        assert col in result.columns
    assert len(result) == len(sample_ohlcv_data)

def test_calculate_indicators_sma(sample_ohlcv_data):
    """Verifies SMA-20 calculation logic."""
    result = calculate_indicators(sample_ohlcv_data)
    
    # For the first row, SMA-20 (min_periods=1) should equal the close
    assert result.iloc[0]["sma_20"] == sample_ohlcv_data.iloc[0]["close"]
    
    # For the 20th row (index 19), it should be the mean of first 20 closes
    expected_sma_20 = sample_ohlcv_data.iloc[0:20]["close"].mean()
    assert pytest.approx(result.iloc[19]["sma_20"]) == expected_sma_20

def test_calculate_indicators_rsi_range(sample_ohlcv_data):
    """Verifies RSI-14 stays within 0-100 range."""
    result = calculate_indicators(sample_ohlcv_data)
    assert result["rsi_14"].min() >= 0
    assert result["rsi_14"].max() <= 100

def test_calculate_sentiment_features():
    """Verifies sentiment polarity shift calculation."""
    data = {
        "ticker": ["AAPL", "AAPL", "AAPL"],
        "window_start": pd.date_range("2023-01-01", periods=3, freq="5min"),
        "avg_sentiment_score": [0.5, 0.7, 0.4]
    }
    df = pd.DataFrame(data)
    result = calculate_sentiment_features(df)
    
    # First row shift should be 0 (diff with self)
    assert result.iloc[0]["sentiment_polarity_shift"] == 0
    # Second row: 0.7 - 0.5 = 0.2
    assert pytest.approx(result.iloc[1]["sentiment_polarity_shift"]) == 0.2
    # Third row: 0.4 - 0.7 = -0.3
    assert pytest.approx(result.iloc[2]["sentiment_polarity_shift"]) == -0.3
