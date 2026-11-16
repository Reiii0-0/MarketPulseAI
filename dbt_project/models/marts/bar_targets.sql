{{ config(materialized='view') }}

WITH anchors AS (
    SELECT *, bar_end + INTERVAL '5 minutes' AS target_at,
           (bar_start AT TIME ZONE 'America/New_York')::date AS session_date_ny
    FROM {{ source('marts', 'final_bars') }}
)
SELECT
    a.contract_id, a.contract_version, a.dataset_id, a.run_id, a.ticker,
    a.bar_start, a.bar_end, a.bar_end + INTERVAL '15 seconds' AS decision_at,
    a.target_at, a.session_date_ny, a.close::DOUBLE PRECISION AS entry_close,
    t.close::DOUBLE PRECISION AS target_close,
    (t.bar_end IS NOT NULL) AS target_is_finalized,
    CASE WHEN t.bar_end IS NOT NULL THEN 'realized' ELSE 'unavailable' END AS target_status,
    CASE
        WHEN (a.target_at AT TIME ZONE 'America/New_York')::time > TIME '16:00:00'
          OR (a.target_at AT TIME ZONE 'America/New_York')::date <> a.session_date_ny
        THEN 'session_boundary'
        WHEN t.bar_end IS NULL THEN 'missing_target_bar'
    END AS unavailable_reason,
    (t.close - a.close)::DOUBLE PRECISION / NULLIF(a.close, 0) AS return_5m,
    CASE WHEN t.bar_end IS NULL THEN NULL WHEN t.close > a.close THEN 1 ELSE 0 END AS target_direction,
    CASE WHEN t.bar_end IS NULL THEN NULL WHEN t.close > a.close THEN 'UP' ELSE 'NOT_UP' END AS target_label
FROM anchors a
LEFT JOIN {{ source('marts', 'final_bars') }} t
  ON t.run_id IS NOT DISTINCT FROM a.run_id
 AND t.contract_id = a.contract_id AND t.dataset_id = a.dataset_id
 AND t.ticker = a.ticker AND t.bar_end = a.target_at
 AND (t.bar_start AT TIME ZONE 'America/New_York')::date = a.session_date_ny
 AND (a.bar_end AT TIME ZONE 'America/New_York')::time > TIME '09:30:00'
 AND (t.bar_end AT TIME ZONE 'America/New_York')::time <= TIME '16:00:00'
