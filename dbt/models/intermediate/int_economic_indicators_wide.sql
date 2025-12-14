{{
    config(
        materialized='view',
        description='Economic indicators in wide format for easy analysis'
    )
}}

/*
    Intermediate model: Economic Indicators Wide
    - Pivots key indicators to columns
    - Calculates year-over-year changes
    - Creates composite indicators
*/

WITH indicators AS (
    SELECT * FROM {{ ref('stg_fred_economic') }}
),

-- Get latest value for each series per month
monthly AS (
    SELECT
        DATE_TRUNC('month', observation_date) AS month,
        series_id,
        value,
        ROW_NUMBER() OVER (
            PARTITION BY series_id, DATE_TRUNC('month', observation_date)
            ORDER BY observation_date DESC
        ) AS rn
    FROM indicators
),

latest_monthly AS (
    SELECT month, series_id, value
    FROM monthly
    WHERE rn = 1
),

pivoted AS (
    SELECT
        month,
        MAX(CASE WHEN series_id = 'GDP' THEN value END) AS gdp,
        MAX(CASE WHEN series_id = 'UNRATE' THEN value END) AS unemployment_rate,
        MAX(CASE WHEN series_id = 'CPIAUCSL' THEN value END) AS cpi,
        MAX(CASE WHEN series_id = 'FEDFUNDS' THEN value END) AS fed_funds_rate,
        MAX(CASE WHEN series_id = 'DGS10' THEN value END) AS treasury_10y,
        MAX(CASE WHEN series_id = 'MORTGAGE30US' THEN value END) AS mortgage_30y,
        MAX(CASE WHEN series_id = 'HOUST' THEN value END) AS housing_starts,
        MAX(CASE WHEN series_id = 'UMCSENT' THEN value END) AS consumer_sentiment,
        MAX(CASE WHEN series_id = 'INDPRO' THEN value END) AS industrial_production
    FROM latest_monthly
    GROUP BY month
),

with_yoy AS (
    SELECT
        p.*,

        -- Year-over-year changes
        LAG(gdp, 12) OVER (ORDER BY month) AS gdp_year_ago,
        LAG(unemployment_rate, 12) OVER (ORDER BY month) AS unrate_year_ago,
        LAG(cpi, 12) OVER (ORDER BY month) AS cpi_year_ago,
        LAG(consumer_sentiment, 12) OVER (ORDER BY month) AS sentiment_year_ago

    FROM pivoted p
)

SELECT
    month,
    gdp,
    unemployment_rate,
    cpi,
    fed_funds_rate,
    treasury_10y,
    mortgage_30y,
    housing_starts,
    consumer_sentiment,
    industrial_production,

    -- YoY GDP growth
    CASE
        WHEN gdp_year_ago > 0
        THEN ROUND((gdp - gdp_year_ago) / gdp_year_ago * 100, 2)
    END AS gdp_yoy_pct,

    -- Inflation rate (YoY CPI change)
    CASE
        WHEN cpi_year_ago > 0
        THEN ROUND((cpi - cpi_year_ago) / cpi_year_ago * 100, 2)
    END AS inflation_rate,

    -- Unemployment change
    ROUND(unemployment_rate - unrate_year_ago, 2) AS unemployment_yoy_change,

    -- Sentiment change
    ROUND(consumer_sentiment - sentiment_year_ago, 2) AS sentiment_yoy_change,

    -- Economic health composite (simplified)
    CASE
        WHEN unemployment_rate < 5 AND consumer_sentiment > 80 THEN 'strong'
        WHEN unemployment_rate < 7 AND consumer_sentiment > 60 THEN 'moderate'
        ELSE 'weak'
    END AS economic_health

FROM with_yoy
WHERE month IS NOT NULL
ORDER BY month DESC
