-- Singular dbt test: fails if bar_targets violates exact +5m timestamp join, finality, or UP label definition.
SELECT
    dataset_id,
    ticker,
    bar_end,
    target_at,
    target_is_finalized,
    target_status,
    return_5m,
    target_direction,
    target_label
FROM {{ ref('bar_targets') }}
WHERE target_at <> bar_end + INTERVAL '5 minutes'
   OR (target_status = 'realized' AND (
       target_is_finalized = FALSE
       OR target_direction IS NULL
       OR return_5m IS NULL
       OR (return_5m > 0 AND (target_direction <> 1 OR target_label <> 'UP'))
       OR (return_5m <= 0 AND (target_direction <> 0 OR target_label <> 'NOT_UP'))
   ))
   OR (target_status = 'unavailable' AND (
       target_direction IS NOT NULL
       OR return_5m IS NOT NULL
   ))
