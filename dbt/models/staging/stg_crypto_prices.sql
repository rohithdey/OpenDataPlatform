{{
    config(
        materialized='view',
        description='Cleaned cryptocurrency price data from CoinGecko'
    )
}}

/*
    Staging model for cryptocurrency prices
    - Standardizes symbols
    - Categorizes by market cap tier
    - Calculates additional metrics
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'crypto_prices') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        TRIM(coin_id) AS coin_id,
        UPPER(TRIM(symbol)) AS symbol,
        TRIM(name) AS coin_name,

        -- Price data
        ROUND(current_price, 6) AS current_price,
        CAST(market_cap AS BIGINT) AS market_cap,
        market_cap_rank,
        CAST(total_volume AS BIGINT) AS volume_24h,

        -- Daily range
        ROUND(high_24h, 6) AS high_24h,
        ROUND(low_24h, 6) AS low_24h,

        -- Price changes
        ROUND(price_change_24h, 6) AS price_change_24h,
        ROUND(price_change_pct_24h, 2) AS price_change_pct_24h,
        ROUND(price_change_pct_7d, 2) AS price_change_pct_7d,
        ROUND(price_change_pct_30d, 2) AS price_change_pct_30d,

        -- Supply metrics
        circulating_supply,
        total_supply,
        max_supply,

        -- All-time highs/lows
        ROUND(ath, 6) AS all_time_high,
        CAST(ath_date AS DATE) AS ath_date,
        ROUND(ath_change_pct, 2) AS pct_from_ath,
        ROUND(atl, 6) AS all_time_low,
        CAST(atl_date AS DATE) AS atl_date,

        -- Market cap tier
        CASE
            WHEN market_cap_rank <= 10 THEN 'mega_cap'
            WHEN market_cap_rank <= 50 THEN 'large_cap'
            WHEN market_cap_rank <= 100 THEN 'mid_cap'
            ELSE 'small_cap'
        END AS market_cap_tier,

        -- Trend indicator
        CASE
            WHEN price_change_pct_24h > 5 THEN 'strong_bullish'
            WHEN price_change_pct_24h > 0 THEN 'bullish'
            WHEN price_change_pct_24h > -5 THEN 'bearish'
            ELSE 'strong_bearish'
        END AS trend_24h,

        -- Metadata
        currency,
        fetch_timestamp,
        CAST(fetch_date AS DATE) AS price_date

    FROM source
    WHERE
        symbol IS NOT NULL
        AND current_price IS NOT NULL
        AND current_price > 0
)

SELECT * FROM cleaned
