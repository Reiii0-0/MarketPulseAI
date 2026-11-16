-- Singular dbt test: fails if any 1-minute bar is outside regular NY session hours or violates bar grace.
SELECT
    dataset_id,
    ticker,
    bar_start,
    bar_end,
    available_at
FROM {{ ref('int_ohlcv_1min') }}
WHERE EXTRACT(ISODOW FROM (bar_start AT TIME ZONE 'America/New_York')) NOT BETWEEN 1 AND 5
   OR (bar_start AT TIME ZONE 'America/New_York')::time < TIME '09:30:00'
   OR ((bar_end - INTERVAL '1 microsecond') AT TIME ZONE 'America/New_York')::time > TIME '15:59:59.999999'
   OR available_at > bar_end + INTERVAL '15 seconds'
