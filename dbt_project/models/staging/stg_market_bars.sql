-- dbt_project/models/staging/stg_market_bars.sql

WITH source AS (
    SELECT * FROM {{ source('raw', 'market_bars') }}
),

validated AS (
    SELECT
        dataset_id,
        source_event_id,
        run_id,
        UPPER(TRIM(ticker)) AS ticker,
        bar_start,
        bar_end,
        event_at,
        available_at,
        open::NUMERIC(12, 4) AS open,
        high::NUMERIC(12, 4) AS high,
        low::NUMERIC(12, 4) AS low,
        close::NUMERIC(12, 4) AS close,
        volume::BIGINT AS volume,
        trade_count::BIGINT AS trade_count,
        vwap::NUMERIC(12, 4) AS vwap,
        observation_type,
        provenance,
        availability_assumed,
        kafka_offset,
        kafka_partition
    FROM source
    WHERE ticker IS NOT NULL
      AND TRIM(ticker) <> ''
      AND open > 0
      AND high > 0
      AND low > 0
      AND close > 0
      AND volume >= 0
      AND high >= GREATEST(open, close)
      AND low <= LEAST(open, close)
      AND low <= high
      AND bar_end = bar_start + INTERVAL '1 minute'
)

SELECT * FROM validated
