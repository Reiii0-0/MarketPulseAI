-- Singular dbt test: fails if warm-up completeness flag is inconsistent with 20-bar contiguity or canonical features are NULL.
SELECT
    dataset_id,
    ticker,
    bar_end,
    contiguous_bar_count,
    warmup_complete
FROM {{ ref('stock_features') }}
WHERE (
    warmup_complete = TRUE AND (
        contiguous_bar_count < 20
        OR open IS NULL
        OR high IS NULL
        OR low IS NULL
        OR close IS NULL
        OR volume IS NULL
        OR return_1m IS NULL
        OR return_5m_past IS NULL
        OR return_15m_past IS NULL
        OR sma_5 IS NULL
        OR sma_20 IS NULL
        OR sma_spread_5_20 IS NULL
        OR volatility_20 IS NULL
        OR volume_ratio_20 IS NULL
        OR bar_range_pct IS NULL
    )
)
OR (
    warmup_complete = FALSE AND (
        contiguous_bar_count >= 20
        OR sma_20 IS NOT NULL
    )
)
