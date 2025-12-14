{{
    config(
        materialized='view',
        description='Treasury yield curve analysis with spread calculations'
    )
}}

/*
    Intermediate model: Treasury Yield Curve
    - Pivots yield data by maturity
    - Calculates spreads
    - Identifies curve inversions
*/

WITH yields AS (
    SELECT * FROM {{ ref('stg_fred_economic') }}
    WHERE series_id IN ('DGS2', 'DGS10', 'FEDFUNDS')
),

pivoted AS (
    SELECT
        observation_date,
        MAX(CASE WHEN series_id = 'FEDFUNDS' THEN value END) AS fed_funds_rate,
        MAX(CASE WHEN series_id = 'DGS2' THEN value END) AS yield_2y,
        MAX(CASE WHEN series_id = 'DGS10' THEN value END) AS yield_10y
    FROM yields
    GROUP BY observation_date
    HAVING yield_2y IS NOT NULL AND yield_10y IS NOT NULL
),

with_spreads AS (
    SELECT
        observation_date,
        fed_funds_rate,
        yield_2y,
        yield_10y,

        -- Yield curve spread (10Y - 2Y)
        ROUND(yield_10y - yield_2y, 4) AS spread_10y_2y,

        -- Term premium (10Y - Fed Funds)
        ROUND(yield_10y - COALESCE(fed_funds_rate, 0), 4) AS term_premium,

        -- Previous values for change calculation
        LAG(yield_2y) OVER (ORDER BY observation_date) AS prev_yield_2y,
        LAG(yield_10y) OVER (ORDER BY observation_date) AS prev_yield_10y,
        LAG(yield_10y - yield_2y) OVER (ORDER BY observation_date) AS prev_spread

    FROM pivoted
),

final AS (
    SELECT
        observation_date,
        fed_funds_rate,
        yield_2y,
        yield_10y,
        spread_10y_2y,
        term_premium,

        -- Daily changes
        ROUND(yield_2y - prev_yield_2y, 4) AS yield_2y_change,
        ROUND(yield_10y - prev_yield_10y, 4) AS yield_10y_change,
        ROUND(spread_10y_2y - prev_spread, 4) AS spread_change,

        -- Curve shape indicator
        CASE
            WHEN spread_10y_2y < 0 THEN 'inverted'
            WHEN spread_10y_2y < 0.5 THEN 'flat'
            WHEN spread_10y_2y < 1.5 THEN 'normal'
            ELSE 'steep'
        END AS curve_shape,

        -- Recession indicator (inversion is historically predictive)
        CASE WHEN spread_10y_2y < 0 THEN TRUE ELSE FALSE END AS is_inverted

    FROM with_spreads
)

SELECT * FROM final
ORDER BY observation_date DESC
