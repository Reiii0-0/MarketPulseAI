-- project/dbt_project/models/intermediate/int_technical_indicators.sql

WITH ohlcv AS (
    SELECT * FROM {{ ref('int_ohlcv_5min') }}
),

-- 1. Moving Averages
mas AS (
    SELECT
        ticker,
        window_start,
        close,
        AVG(close) OVER (PARTITION BY ticker ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20
    FROM ohlcv
),

-- 2. Exponential Moving Averages (EMA)
-- PostgreSQL doesn't have a built-in EMA function, using the standard formula.
-- For EMA-12 and EMA-26, we'll use a simplified approximation or recursive CTE.
-- For v1.0, we'll use a common EMA approximation with window functions.
emas AS (
    SELECT
        ticker,
        window_start,
        close,
        sma_20,
        -- Note: These are approximations using weighted windows. 
        -- Real EMA is recursive. This is acceptable for Sprint 0.
        AVG(close) OVER (PARTITION BY ticker ORDER BY window_start ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS ema_12,
        AVG(close) OVER (PARTITION BY ticker ORDER BY window_start ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) AS ema_26
    FROM mas
),

-- 3. MACD
macd AS (
    SELECT
        *,
        (ema_12 - ema_26) AS macd_line
    FROM emas
),

macd_signal AS (
    SELECT
        *,
        AVG(macd_line) OVER (PARTITION BY ticker ORDER BY window_start ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS macd_signal
    FROM macd
),

macd_final AS (
    SELECT
        *,
        (macd_line - macd_signal) AS macd_histogram
    FROM macd_signal
),

-- 4. RSI-14
rsi_base AS (
    SELECT
        ticker,
        window_start,
        close,
        LAG(close) OVER (PARTITION BY ticker ORDER BY window_start) AS prev_close
    FROM ohlcv
),

rsi_diff AS (
    SELECT
        *,
        COALESCE(close - prev_close, 0) AS diff,
        CASE WHEN close - prev_close > 0 THEN close - prev_close ELSE 0 END AS gain,
        CASE WHEN close - prev_close < 0 THEN ABS(close - prev_close) ELSE 0 END AS loss
    FROM rsi_base
),

rsi_avg AS (
    SELECT
        *,
        AVG(gain) OVER (PARTITION BY ticker ORDER BY window_start ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY ticker ORDER BY window_start ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM rsi_diff
),

rsi_final AS (
    SELECT
        ticker,
        window_start,
        CASE 
            WHEN avg_loss = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
        END AS rsi_14
    FROM rsi_avg
)

-- Join everything back
SELECT
    m.ticker,
    m.window_start,
    m.sma_20::NUMERIC(12, 4),
    m.ema_12::NUMERIC(12, 4),
    m.ema_26::NUMERIC(12, 4),
    r.rsi_14::NUMERIC(7, 4),
    m.macd_line::NUMERIC(10, 6),
    m.macd_signal::NUMERIC(10, 6),
    m.macd_histogram::NUMERIC(10, 6)
FROM macd_final m
JOIN rsi_final r ON m.ticker = r.ticker AND m.window_start = r.window_start
