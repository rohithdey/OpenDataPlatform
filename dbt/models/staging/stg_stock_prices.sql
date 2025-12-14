{{
    config(
        materialized='view',
        description='Cleaned and standardized stock price data from Yahoo Finance'
    )
}}

/*
    Staging model for stock prices
    - Standardizes column names
    - Filters out invalid records
    - Adds computed fields
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'stock_prices') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        UPPER(TRIM(symbol)) AS symbol,

        -- Date handling
        CAST(date AS DATE) AS price_date,

        -- Price data (ensure positive values)
        CASE WHEN open > 0 THEN ROUND(open, 4) END AS open_price,
        CASE WHEN high > 0 THEN ROUND(high, 4) END AS high_price,
        CASE WHEN low > 0 THEN ROUND(low, 4) END AS low_price,
        CASE WHEN close > 0 THEN ROUND(close, 4) END AS close_price,

        -- Volume
        CAST(volume AS BIGINT) AS volume,

        -- Metadata
        fetch_timestamp,
        ingestion_date,

        -- Data quality flag
        CASE
            WHEN close > 0 AND high >= low AND high >= close AND low <= close
            THEN TRUE
            ELSE FALSE
        END AS is_valid_ohlc

    FROM source
    WHERE
        symbol IS NOT NULL
        AND date IS NOT NULL
        AND close IS NOT NULL
        AND close > 0
)

SELECT * FROM cleaned
WHERE is_valid_ohlc = TRUE
