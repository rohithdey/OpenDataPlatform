{{
    config(
        materialized='table',
        description='Stock summary with latest prices, returns, and key metrics'
    )
}}

/*
    Mart: Stock Summary
    Final analytics table for stock overview
*/

WITH metrics AS (
    SELECT * FROM {{ ref('int_stock_daily_metrics') }}
),

latest_date AS (
    SELECT MAX(price_date) AS max_date
    FROM metrics
),

latest_prices AS (
    SELECT m.*
    FROM metrics m
    CROSS JOIN latest_date ld
    WHERE m.price_date = ld.max_date
),

-- Get weekly and monthly returns
period_returns AS (
    SELECT
        symbol,
        -- 5-day return
        (LAST_VALUE(close_price) OVER w - FIRST_VALUE(close_price) OVER w)
            / NULLIF(FIRST_VALUE(close_price) OVER w, 0) * 100 AS return_5d,
        -- 20-day return
        (LAST_VALUE(close_price) OVER w20 - FIRST_VALUE(close_price) OVER w20)
            / NULLIF(FIRST_VALUE(close_price) OVER w20, 0) * 100 AS return_20d,
        price_date,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY price_date DESC) AS rn
    FROM metrics
    WINDOW
        w AS (PARTITION BY symbol ORDER BY price_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY symbol ORDER BY price_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
),

latest_returns AS (
    SELECT symbol, return_5d, return_20d
    FROM period_returns
    WHERE rn = 1
)

SELECT
    lp.symbol,
    lp.price_date AS last_updated,
    lp.close_price AS current_price,
    lp.open_price,
    lp.high_price,
    lp.low_price,
    lp.volume,

    -- Daily metrics
    lp.daily_return_pct,
    lp.intraday_range_pct,

    -- Period returns
    ROUND(lr.return_5d, 2) AS return_5d_pct,
    ROUND(lr.return_20d, 2) AS return_20d_pct,

    -- Technical indicators
    lp.moving_avg_20,
    lp.moving_avg_50,
    lp.pct_above_ma20,
    lp.pct_above_ma50,
    lp.volatility_20,
    lp.trend_signal,

    -- Volume analysis
    lp.avg_volume_20,
    lp.relative_volume,

    -- Performance ranking
    RANK() OVER (ORDER BY lp.daily_return_pct DESC) AS daily_return_rank,
    RANK() OVER (ORDER BY lr.return_5d DESC) AS weekly_return_rank,
    RANK() OVER (ORDER BY lp.volatility_20 DESC) AS volatility_rank

FROM latest_prices lp
LEFT JOIN latest_returns lr ON lp.symbol = lr.symbol
ORDER BY lp.symbol
