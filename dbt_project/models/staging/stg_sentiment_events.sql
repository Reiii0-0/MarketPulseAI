-- dbt_project/models/staging/stg_sentiment_events.sql

WITH source AS (
    SELECT * FROM {{ source('raw', 'sentiment_events') }}
),

renamed AS (
    SELECT
        dataset_id,
        source_event_id,
        run_id,
        UPPER(TRIM(ticker)) AS ticker,
        COALESCE(event_at, event_timestamp) AS event_at,
        COALESCE(available_at, event_timestamp) AS available_at,
        event_timestamp,
        source AS event_source,
        headline,
        sentiment_score::NUMERIC(5, 4) AS sentiment_score,
        sentiment_label,
        confidence::NUMERIC(5, 4) AS confidence,
        observation_type,
        provenance,
        kafka_offset,
        kafka_partition
    FROM source
    WHERE sentiment_score BETWEEN -1.0 AND 1.0
      AND ticker IS NOT NULL
      AND TRIM(ticker) <> ''
)

SELECT * FROM renamed
