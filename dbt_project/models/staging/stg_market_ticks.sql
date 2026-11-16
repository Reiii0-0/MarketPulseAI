-- dbt_project/models/staging/stg_market_ticks.sql

WITH source AS (
    SELECT * FROM {{ source('raw', 'market_ticks') }}
),

renamed AS (
    SELECT
        dataset_id,
        source_event_id,
        run_id,
        UPPER(TRIM(ticker)) AS ticker,
        COALESCE(event_at, event_timestamp) AT TIME ZONE 'UTC' AS event_at_utc,
        COALESCE(event_at, event_timestamp) AS event_at,
        COALESCE(available_at, event_timestamp) AS available_at,
        event_timestamp,
        price::NUMERIC(12, 4) AS price,
        bid::NUMERIC(12, 4) AS bid,
        ask::NUMERIC(12, 4) AS ask,
        volume::BIGINT AS volume,
        trade_condition,
        observation_type,
        provenance,
        kafka_offset,
        kafka_partition
    FROM source
    WHERE price IS NOT NULL
      AND price > 0
      AND volume IS NOT NULL
      AND volume >= 0
      AND ticker IS NOT NULL
      AND TRIM(ticker) <> ''
)

SELECT * FROM renamed
