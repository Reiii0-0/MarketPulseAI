-- dbt_project/models/marts/stock_features.sql
-- Pure canonical feature view for mp_1m_5m_price_v1 (ZERO future LEAD/target columns).
-- Warm-up rows (< 20 contiguous session bars) have warmup_complete = FALSE.

{{ config(materialized='view') }}

SELECT
    contract_id,
    contract_version,
    dataset_id,
    run_id,
    ticker,
    bar_start,
    bar_end,
    bar_end + INTERVAL '15 seconds' AS decision_at,
    available_at,
    session_date_ny,
    segment_id,
    contiguous_bar_count,
    warmup_complete,
    'price_only'::VARCHAR(32) AS feature_mode,
    open,
    high,
    low,
    close,
    volume,
    return_1m,
    return_5m_past,
    return_15m_past,
    sma_5,
    sma_20,
    sma_spread_5_20,
    volatility_20,
    volume_ratio_20,
    bar_range_pct
FROM {{ ref('int_technical_indicators') }}
