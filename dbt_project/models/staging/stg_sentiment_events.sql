-- project/dbt_project/models/staging/stg_sentiment_events.sql

WITH source AS (
    SELECT * FROM {{ source('raw', 'sentiment_events') }}
),

renamed AS (
    SELECT
        ticker,
        event_timestamp,
        source AS event_source,
        headline,
        sentiment_score::NUMERIC(5, 4) AS sentiment_score,
        sentiment_label,
        confidence::NUMERIC(5, 4) AS confidence,
        kafka_offset,
        kafka_partition
    FROM source
    WHERE sentiment_score BETWEEN -1.0 AND 1.0
      AND ticker IS NOT NULL
)

SELECT * FROM renamed
