{{
    config(
        materialized='view',
        description='Daily stock metrics with returns, moving averages, and volatility'
    )
}}

/*
    Intermediate model: Stock Daily Metrics
    - Calculates daily returns
    - Computes moving averages
    - Calculates rolling volatility
*/

WITH prices AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

with_lag AS (
    SELECT
        symbol,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,

        -- Previous day's close for return calculation
        LAG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY price_date
        ) AS prev_close,

        -- Previous day's volume
        LAG(volume) OVER (
            PARTITION BY symbol
            ORDER BY price_date
        ) AS prev_volume

    FROM prices
),

with_returns AS (
    SELECT
        *,

        -- Daily return
        CASE
            WHEN prev_close > 0
            THEN ROUND((close_price - prev_close) / prev_close * 100, 4)
        END AS daily_return_pct,

        -- Volume change
        CASE
            WHEN prev_volume > 0
            THEN ROUND((volume - prev_volume) / prev_volume * 100, 2)
        END AS volume_change_pct,

        -- Intraday range
        ROUND((high_price - low_price) / NULLIF(low_price, 0) * 100, 2) AS intraday_range_pct

    FROM with_lag
),

with_moving_avgs AS (
    SELECT
        *,

        -- 20-day moving average (configurable via var)
        ROUND(AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN {{ var('moving_avg_window') - 1 }} PRECEDING AND CURRENT ROW
        ), 4) AS moving_avg_20,

        -- 50-day moving average
        ROUND(AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ), 4) AS moving_avg_50,

        -- 20-day average volume
        ROUND(AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0) AS avg_volume_20

    FROM with_returns
),

with_volatility AS (
    SELECT
        *,

        -- 20-day rolling volatility (standard deviation of returns)
        ROUND(STDDEV(daily_return_pct) OVER (
            PARTITION BY symbol
            ORDER BY price_date
            ROWS BETWEEN {{ var('volatility_window') - 1 }} PRECEDING AND CURRENT ROW
        ), 4) AS volatility_20,

        -- Price relative to moving averages
        ROUND((close_price - moving_avg_20) / NULLIF(moving_avg_20, 0) * 100, 2) AS pct_above_ma20,
        ROUND((close_price - moving_avg_50) / NULLIF(moving_avg_50, 0) * 100, 2) AS pct_above_ma50,

        -- Volume relative to average
        ROUND(volume / NULLIF(avg_volume_20, 0), 2) AS relative_volume

    FROM with_moving_avgs
)

SELECT
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    daily_return_pct,
    intraday_range_pct,
    volume_change_pct,
    moving_avg_20,
    moving_avg_50,
    pct_above_ma20,
    pct_above_ma50,
    volatility_20,
    avg_volume_20,
    relative_volume,

    -- Trend signals
    CASE
        WHEN close_price > moving_avg_20 AND moving_avg_20 > moving_avg_50 THEN 'bullish'
        WHEN close_price < moving_avg_20 AND moving_avg_20 < moving_avg_50 THEN 'bearish'
        ELSE 'neutral'
    END AS trend_signal

FROM with_volatility
