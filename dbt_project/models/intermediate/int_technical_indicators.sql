-- Serving SQL in pipeline/features.py freezes these values once. dbt exposes the same snapshots.
SELECT
    s.contract_id, s.contract_version, s.dataset_id, s.run_id, s.ticker,
    s.bar_start, s.bar_end, b.available_at,
    (s.bar_start AT TIME ZONE 'America/New_York')::date AS session_date_ny,
    DENSE_RANK() OVER (
        PARTITION BY s.run_id, s.contract_id, s.dataset_id, s.ticker
        ORDER BY s.bar_end - s.contiguous_bar_count * INTERVAL '1 minute'
    ) AS segment_id,
    s.contiguous_bar_count, s.warmup_complete,
    s.open, s.high, s.low, s.close, s.volume, s.return_1m, s.return_5m_past, s.return_15m_past, s.sma_5, s.sma_20, s.sma_spread_5_20, s.volatility_20, s.volume_ratio_20, s.bar_range_pct
FROM {{ source('marts', 'feature_snapshots') }} s
JOIN {{ source('marts', 'final_bars') }} b
  ON b.run_id IS NOT DISTINCT FROM s.run_id
 AND b.contract_id = s.contract_id AND b.dataset_id = s.dataset_id
 AND b.ticker = s.ticker AND b.bar_end = s.bar_end
