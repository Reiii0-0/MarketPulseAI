-- project/dbt_project/models/intermediate/int_ohlcv_5min.sql

WITH base_ticks AS (
    SELECT
        ticker,
        event_timestamp,
        price,
        volume,
        DATE_TRUNC('minute', event_timestamp) AS window_start
    FROM {{ ref('stg_market_ticks') }}
),

windowed AS (
    SELECT
        ticker,
        window_start,
        price,
        volume,
        FIRST_VALUE(price) OVER (
            PARTITION BY ticker, window_start 
            ORDER BY event_timestamp ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS open_price,
        LAST_VALUE(price) OVER (
            PARTITION BY ticker, window_start 
            ORDER BY event_timestamp ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS close_price
    FROM base_ticks
),

aggregated AS (
    SELECT
        ticker,
        window_start,
        window_start + INTERVAL '1 minute' AS window_end,
        MIN(open_price) AS open, -- MIN works here because open_price is same for all rows in window
        MAX(price) AS high,
        MIN(price) AS low,
        MAX(close_price) AS close, -- MAX works here because close_price is same for all rows in window
        SUM(volume) AS volume
    FROM windowed
    GROUP BY 1, 2, 3
)

SELECT * FROM aggregated
