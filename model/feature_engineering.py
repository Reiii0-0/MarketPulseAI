from __future__ import annotations

import pandas as pd


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Python implementation of technical indicators mirroring dbt logic.

    Args:
        df (pd.DataFrame): OHLCV data. Must contain 'ticker', 'window_start', 'close'.
            Expected to be sorted by window_start ASC.

    Returns:
        pd.DataFrame: DataFrame with indicators added as columns.
    """
    df = df.copy()

    # Ensure sorting for rolling calculations
    df = df.sort_values(["ticker", "window_start"])

    # 1. SMA-20
    df["sma_20"] = df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(window=20, min_periods=1).mean()
    )

    # 2. EMA-12 & EMA-26
    # Note: Using ewm (exponential weighted moving average) with span.
    # To match the dbt "approximation", we could use rolling mean, but ewm is standard.
    # However, to maintain STRICT parity with the dbt SQL (which used AVG over 12/26 rows),
    # we will use rolling mean if we want to be identical, or ewm if we want to be "correct".
    # The dbt SQL used: AVG(close) OVER (... ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
    # So we MUST use rolling().mean() to maintain perfect parity as requested.
    df["ema_12"] = df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(window=12, min_periods=1).mean()
    )
    df["ema_26"] = df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(window=26, min_periods=1).mean()
    )

    # 3. MACD
    df["macd_line"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df.groupby("ticker")["macd_line"].transform(
        lambda x: x.rolling(window=9, min_periods=1).mean()
    )
    df["macd_histogram"] = df["macd_line"] - df["macd_signal"]

    # 4. RSI-14
    def rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta = series.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)

        avg_gain = gain.rolling(window=period, min_periods=1).mean()
        avg_loss = loss.rolling(window=period, min_periods=1).mean()

        rs = avg_gain / avg_loss
        rsi_val = 100 - (100 / (1 + rs))
        return rsi_val.fillna(100)  # Handle div by zero like SQL CASE

    df["rsi_14"] = df.groupby("ticker")["close"].transform(lambda x: rsi(x, 14))

    return df


def calculate_sentiment_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate rolling sentiment polarity shift.

    Args:
        df (pd.DataFrame): Data containing 'ticker', 'avg_sentiment_score'.

    Returns:
        pd.DataFrame: DataFrame with 'sentiment_polarity_shift'.
    """
    df = df.copy()
    df = df.sort_values(["ticker", "window_start"])

    df["prev_avg_sentiment_score"] = df.groupby("ticker")["avg_sentiment_score"].shift(1)
    df["sentiment_polarity_shift"] = (
        df["avg_sentiment_score"] - df["prev_avg_sentiment_score"].fillna(df["avg_sentiment_score"])
    )

    return df
