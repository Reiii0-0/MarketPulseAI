-- dbt_project/models/intermediate/int_ohlcv_1min.sql
-- Canonical 1-minute UTC OHLCV bars filtered to regular America/New_York session
-- and guarded by the mp_1m_5m_price_v1 15-second bar grace cutoff.

WITH bar_feed_candidates AS (
    SELECT
        dataset_id,
        run_id,
        ticker,
        bar_start,
        bar_end,
        GREATEST(available_at, bar_end) AS available_at,
        open,
        high,
        low,
        close,
        volume,
        COALESCE(trade_count, 1)::INTEGER AS event_count,
        provenance,
        ROW_NUMBER() OVER (
            PARTITION BY dataset_id, ticker, bar_end
            ORDER BY available_at ASC, source_event_id ASC
        ) AS rn
    FROM {{ ref('stg_market_bars') }}
    WHERE available_at <= bar_end + INTERVAL '15 seconds'
),

direct_bars AS (
    SELECT
        dataset_id,
        run_id,
        ticker,
        bar_start,
        bar_end,
        available_at,
        open,
        high,
        low,
        close,
        volume,
        event_count,
        provenance
    FROM bar_feed_candidates
    WHERE rn = 1
),

ticks_bucketed AS (
    SELECT
        dataset_id,
        run_id,
        ticker,
        DATE_TRUNC('minute', event_at) AS bar_start,
        DATE_TRUNC('minute', event_at) + INTERVAL '1 minute' AS bar_end,
        event_at,
        available_at,
        source_event_id,
        price,
        volume,
        provenance
    FROM {{ ref('stg_market_ticks') }}
    WHERE available_at <= DATE_TRUNC('minute', event_at) + INTERVAL '1 minute 15 seconds'
),

ticks_windowed AS (
    SELECT
        dataset_id,
        run_id,
        ticker,
        bar_start,
        bar_end,
        available_at,
        FIRST_VALUE(price) OVER (
            PARTITION BY dataset_id, ticker, bar_start
            ORDER BY event_at ASC, source_event_id ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS open,
        MAX(price) OVER (
            PARTITION BY dataset_id, ticker, bar_start
        ) AS high,
        MIN(price) OVER (
            PARTITION BY dataset_id, ticker, bar_start
        ) AS low,
        LAST_VALUE(price) OVER (
            PARTITION BY dataset_id, ticker, bar_start
            ORDER BY event_at ASC, source_event_id ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS close,
        SUM(volume) OVER (
            PARTITION BY dataset_id, ticker, bar_start
        ) AS sum_volume,
        COUNT(*) OVER (
            PARTITION BY dataset_id, ticker, bar_start
        ) AS event_count,
        provenance
    FROM ticks_bucketed
),

tick_bars AS (
    SELECT
        dataset_id,
        MAX(run_id) AS run_id,
        ticker,
        bar_start,
        bar_end,
        GREATEST(MAX(available_at), bar_end) AS available_at,
        MAX(open)::NUMERIC(12, 4) AS open,
        MAX(high)::NUMERIC(12, 4) AS high,
        MAX(low)::NUMERIC(12, 4) AS low,
        MAX(close)::NUMERIC(12, 4) AS close,
        MAX(sum_volume)::BIGINT AS volume,
        MAX(event_count)::INTEGER AS event_count,
        MIN(provenance) AS provenance
    FROM ticks_windowed
    GROUP BY dataset_id, ticker, bar_start, bar_end
),

combined AS (
    SELECT
        COALESCE(d.dataset_id, t.dataset_id) AS dataset_id,
        COALESCE(d.run_id, t.run_id) AS run_id,
        COALESCE(d.ticker, t.ticker) AS ticker,
        COALESCE(d.bar_start, t.bar_start) AS bar_start,
        COALESCE(d.bar_end, t.bar_end) AS bar_end,
        COALESCE(d.available_at, t.available_at) AS available_at,
        COALESCE(d.open, t.open) AS open,
        COALESCE(d.high, t.high) AS high,
        COALESCE(d.low, t.low) AS low,
        COALESCE(d.close, t.close) AS close,
        COALESCE(d.volume, t.volume) AS volume,
        COALESCE(d.event_count, t.event_count) AS event_count,
        COALESCE(d.provenance, t.provenance) AS provenance
    FROM direct_bars d
    FULL OUTER JOIN tick_bars t
      ON d.dataset_id = t.dataset_id
     AND d.ticker = t.ticker
     AND d.bar_end = t.bar_end
),

session_filtered AS (
    SELECT
        'mp_1m_5m_price_v1'::VARCHAR(64) AS contract_id,
        '1.0.0'::VARCHAR(32) AS contract_version,
        dataset_id,
        run_id,
        ticker,
        bar_start,
        bar_end,
        available_at,
        (bar_start AT TIME ZONE 'America/New_York')::date AS session_date_ny,
        open,
        high,
        low,
        close,
        volume,
        event_count,
        provenance
    FROM combined
    WHERE EXTRACT(ISODOW FROM (bar_start AT TIME ZONE 'America/New_York')) BETWEEN 1 AND 5
      AND (bar_start AT TIME ZONE 'America/New_York')::date
          = ((bar_end - INTERVAL '1 microsecond') AT TIME ZONE 'America/New_York')::date
      AND (bar_start AT TIME ZONE 'America/New_York')::time >= TIME '09:30:00'
      AND ((bar_end - INTERVAL '1 microsecond') AT TIME ZONE 'America/New_York')::time <= TIME '15:59:59.999999'
      AND bar_end = bar_start + INTERVAL '1 minute'
)

SELECT * FROM session_filtered
