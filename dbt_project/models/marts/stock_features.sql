-- project/dbt_project/models/marts/stock_features.sql

WITH ohlcv AS (
    SELECT * FROM {{ ref('int_ohlcv_5min') }}
),

sentiment AS (
    SELECT * FROM {{ ref('int_sentiment_agg') }}
),

indicators AS (
    SELECT * FROM {{ ref('int_technical_indicators') }}
),

joined AS (
    SELECT
        o.ticker,
        o.window_start,
        o.window_end,
        o.open,
        o.high,
        o.low,
        o.close,
        o.volume,
        i.sma_20,
        i.ema_12,
        i.ema_26,
        i.rsi_14,
        i.macd_line,
        i.macd_signal,
        i.macd_histogram,
        s.avg_sentiment_score,
        s.sentiment_event_count,
        s.sentiment_polarity_shift
    FROM ohlcv o
    LEFT JOIN indicators i ON o.ticker = i.ticker AND o.window_start = i.window_start
    LEFT JOIN sentiment s ON o.ticker = s.ticker AND o.window_start = s.window_start
),

with_target AS (
    SELECT
        *,
        CASE 
            WHEN LEAD(close) OVER (PARTITION BY ticker ORDER BY window_start) > close THEN 1
            ELSE 0
        END AS target_direction
    FROM joined
)

SELECT * FROM with_target
