{{
    config(
        materialized='table',
        description='Daily market overview combining stocks, crypto, and economic indicators'
    )
}}

/*
    Mart: Market Overview
    Combines all asset classes for a unified market view
*/

WITH stocks AS (
    SELECT
        'stock' AS asset_class,
        symbol,
        symbol AS name,
        price_date,
        close_price AS price,
        daily_return_pct,
        volatility_20 AS volatility,
        volume,
        trend_signal
    FROM {{ ref('int_stock_daily_metrics') }}
    WHERE price_date = (SELECT MAX(price_date) FROM {{ ref('int_stock_daily_metrics') }})
),

crypto AS (
    SELECT
        'crypto' AS asset_class,
        symbol,
        coin_name AS name,
        price_date,
        current_price AS price,
        price_change_pct_24h AS daily_return_pct,
        NULL AS volatility,
        volume_24h AS volume,
        trend_24h AS trend_signal
    FROM {{ ref('stg_crypto_prices') }}
    WHERE price_date = (SELECT MAX(price_date) FROM {{ ref('stg_crypto_prices') }})
),

yields AS (
    SELECT
        observation_date,
        yield_2y,
        yield_10y,
        spread_10y_2y,
        curve_shape
    FROM {{ ref('int_treasury_yield_curve') }}
    ORDER BY observation_date DESC
    LIMIT 1
),

combined AS (
    SELECT * FROM stocks
    UNION ALL
    SELECT * FROM crypto
)

SELECT
    c.asset_class,
    c.symbol,
    c.name,
    c.price_date,
    c.price,
    c.daily_return_pct,
    c.volatility,
    c.volume,
    c.trend_signal,

    -- Market context from yields
    y.yield_10y AS treasury_10y,
    y.spread_10y_2y AS yield_curve_spread,
    y.curve_shape,

    -- Performance category
    CASE
        WHEN c.daily_return_pct > 3 THEN 'strong_gainer'
        WHEN c.daily_return_pct > 0 THEN 'gainer'
        WHEN c.daily_return_pct > -3 THEN 'loser'
        ELSE 'strong_loser'
    END AS performance_category

FROM combined c
CROSS JOIN yields y
ORDER BY c.asset_class, ABS(c.daily_return_pct) DESC
