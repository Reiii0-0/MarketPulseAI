-- project/dbt_project/models/intermediate/int_sentiment_agg.sql

WITH base_sentiment AS (
    SELECT
        ticker,
        event_timestamp,
        sentiment_score,
        DATE_TRUNC('minute', event_timestamp) AS window_start
    FROM {{ ref('stg_sentiment_events') }}
),

aggregated AS (
    SELECT
        ticker,
        window_start,
        AVG(sentiment_score)::NUMERIC(5, 4) AS avg_sentiment_score,
        COUNT(*)::INTEGER AS sentiment_event_count
    FROM base_sentiment
    GROUP BY 1, 2
),

with_lag AS (
    SELECT
        *,
        LAG(avg_sentiment_score) OVER (PARTITION BY ticker ORDER BY window_start) AS prev_avg_sentiment_score
    FROM aggregated
),

final AS (
    SELECT
        ticker,
        window_start,
        avg_sentiment_score,
        sentiment_event_count,
        (avg_sentiment_score - COALESCE(prev_avg_sentiment_score, avg_sentiment_score))::NUMERIC(5, 4) AS sentiment_polarity_shift
    FROM with_lag
)

SELECT * FROM final
