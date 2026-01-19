-- project/dbt_project/models/marts/predictions.sql

-- This model acts as a pass-through for the predictions table managed by the API.
-- We materialize it as a view so dbt can use it as a dependency for monitoring models
-- without trying to recreate the underlying table.

{{ config(materialized='view') }}

SELECT
    id,
    request_id,
    ticker,
    predicted_at,
    window_start,
    predicted_direction,
    probability_up,
    probability_down,
    confidence_tier,
    shap_factors,
    actual_direction,
    was_correct,
    model_version
FROM {{ source('raw', 'predictions') }}
