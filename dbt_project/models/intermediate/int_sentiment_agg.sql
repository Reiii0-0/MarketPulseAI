-- dbt_project/models/intermediate/int_sentiment_agg.sql
-- Auxiliary per-minute sentiment aggregation aligned by bar_end.
-- Unavailable sentiment remains NULL (distinct from neutral 0.0) and is excluded from canonical price_only model features.

WITH base_sentiment AS (
    SELECT
        dataset_id,
        ticker,
        event_at,
        available_at,
        sentiment_score,
        DATE_TRUNC('minute', available_at) + INTERVAL '1 minute' AS bar_end
    FROM {{ ref('stg_sentiment_events') }}
    WHERE available_at <= DATE_TRUNC('minute', available_at) + INTERVAL '1 minute 15 seconds'
),

aggregated AS (
    SELECT
        dataset_id,
        ticker,
        bar_end,
        AVG(sentiment_score)::NUMERIC(5, 4) AS avg_sentiment_score,
        COUNT(*)::INTEGER AS sentiment_event_count
    FROM base_sentiment
    GROUP BY dataset_id, ticker, bar_end
),

with_lag AS (
    SELECT
        *,
        LAG(avg_sentiment_score) OVER (
            PARTITION BY dataset_id, ticker
            ORDER BY bar_end ASC
        ) AS prev_avg_sentiment_score
    FROM aggregated
),

final AS (
    SELECT
        dataset_id,
        ticker,
        bar_end,
        avg_sentiment_score,
        sentiment_event_count,
        CASE
            WHEN prev_avg_sentiment_score IS NOT NULL
            THEN (avg_sentiment_score - prev_avg_sentiment_score)::NUMERIC(5, 4)
        END AS sentiment_polarity_shift
    FROM with_lag
)

SELECT * FROM final
