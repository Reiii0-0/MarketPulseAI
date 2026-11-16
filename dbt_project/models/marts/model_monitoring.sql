-- dbt_project/models/marts/model_monitoring.sql

WITH base_predictions AS (
    SELECT
        ticker,
        DATE_TRUNC('hour', predicted_at) AS monitoring_window_start,
        was_correct,
        CASE WHEN predicted_direction = 1 THEN probability_up ELSE probability_down END AS confidence_val,
        confidence_tier
    FROM {{ ref('predictions') }}
    WHERE actual_direction IS NOT NULL
),

aggregated AS (
    SELECT
        monitoring_window_start,
        monitoring_window_start + INTERVAL '1 hour' AS monitoring_window_end,
        ticker,
        COUNT(*) AS total_predictions,
        SUM(CASE WHEN was_correct THEN 1 ELSE 0 END) AS correct_predictions,
        AVG(confidence_val)::NUMERIC(6, 4) AS avg_confidence,
        SUM(CASE WHEN confidence_tier = 'HIGH' THEN 1 ELSE 0 END) AS high_confidence_count,
        SUM(CASE WHEN confidence_tier = 'LOW' THEN 1 ELSE 0 END) AS low_confidence_count
    FROM base_predictions
    GROUP BY 1, 2, 3
)

SELECT
    *,
    (correct_predictions::FLOAT / NULLIF(total_predictions, 0))::NUMERIC(6, 4) AS accuracy
FROM aggregated
