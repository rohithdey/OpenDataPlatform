{{
    config(
        materialized='table',
        description='Economic dashboard with key indicators and trend analysis'
    )
}}

/*
    Mart: Economic Dashboard
    Key economic indicators for macro analysis
*/

WITH indicators AS (
    SELECT * FROM {{ ref('int_economic_indicators_wide') }}
    WHERE month IS NOT NULL
    ORDER BY month DESC
    LIMIT 24  -- Last 2 years
),

yield_curve AS (
    SELECT
        observation_date,
        yield_2y,
        yield_10y,
        spread_10y_2y,
        curve_shape,
        is_inverted
    FROM {{ ref('int_treasury_yield_curve') }}
    ORDER BY observation_date DESC
    LIMIT 1
),

latest AS (
    SELECT * FROM indicators
    ORDER BY month DESC
    LIMIT 1
),

previous_month AS (
    SELECT * FROM indicators
    ORDER BY month DESC
    LIMIT 1 OFFSET 1
)

SELECT
    -- Current period
    l.month AS report_month,
    l.gdp,
    l.unemployment_rate,
    l.inflation_rate,
    l.fed_funds_rate,
    l.consumer_sentiment,
    l.economic_health,

    -- Month-over-month changes
    ROUND(l.unemployment_rate - p.unemployment_rate, 2) AS unemployment_mom_change,
    ROUND(l.inflation_rate - p.inflation_rate, 2) AS inflation_mom_change,
    ROUND(l.consumer_sentiment - p.consumer_sentiment, 2) AS sentiment_mom_change,

    -- Year-over-year
    l.gdp_yoy_pct,
    l.unemployment_yoy_change,
    l.sentiment_yoy_change,

    -- Yield curve
    y.yield_2y,
    y.yield_10y,
    y.spread_10y_2y,
    y.curve_shape,
    y.is_inverted AS yield_curve_inverted,

    -- Trend indicators
    CASE
        WHEN l.unemployment_rate < p.unemployment_rate THEN 'improving'
        WHEN l.unemployment_rate > p.unemployment_rate THEN 'worsening'
        ELSE 'stable'
    END AS employment_trend,

    CASE
        WHEN l.inflation_rate > 3 THEN 'high'
        WHEN l.inflation_rate > 2 THEN 'moderate'
        WHEN l.inflation_rate > 0 THEN 'low'
        ELSE 'deflationary'
    END AS inflation_level,

    -- Recession risk score (simplified)
    CASE
        WHEN y.is_inverted AND l.consumer_sentiment < 70 THEN 'high'
        WHEN y.is_inverted OR l.consumer_sentiment < 70 THEN 'elevated'
        WHEN l.unemployment_rate > 5 THEN 'moderate'
        ELSE 'low'
    END AS recession_risk

FROM latest l
CROSS JOIN previous_month p
CROSS JOIN yield_curve y
