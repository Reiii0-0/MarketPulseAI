-- project/dbt_project/models/staging/stg_market_ticks.sql

WITH source AS (
    SELECT * FROM {{ source('raw', 'market_ticks') }}
),

renamed AS (
    SELECT
        ticker,
        event_timestamp,
        price::NUMERIC(12, 4) AS price,
        bid::NUMERIC(12, 4) AS bid,
        ask::NUMERIC(12, 4) AS ask,
        volume::BIGINT AS volume,
        trade_condition,
        kafka_offset,
        kafka_partition
    FROM source
    WHERE price IS NOT NULL
      AND volume IS NOT NULL
      AND ticker IS NOT NULL
)

SELECT * FROM renamed
