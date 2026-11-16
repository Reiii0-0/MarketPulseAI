-- Singular dbt test: fails if any canonical 1-minute bar violates OHLCV range/ordering bounds.
SELECT
    dataset_id,
    ticker,
    bar_start,
    bar_end,
    open,
    high,
    low,
    close,
    volume
FROM {{ ref('int_ohlcv_1min') }}
WHERE open <= 0
   OR high <= 0
   OR low <= 0
   OR close <= 0
   OR volume < 0
   OR high < GREATEST(open, close)
   OR low > LEAST(open, close)
   OR low > high
   OR bar_end <> bar_start + INTERVAL '1 minute'
