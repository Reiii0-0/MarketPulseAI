{{ config(materialized='view') }}

-- Runtime migration owns outcome semantics.
SELECT * FROM {{ source('marts', 'realized_outcomes_v') }}
