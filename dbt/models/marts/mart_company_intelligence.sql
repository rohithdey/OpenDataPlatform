{{
    config(
        materialized='table',
        description='Company intelligence combining stock performance with SEC filings'
    )
}}

/*
    Mart: Company Intelligence
    Combines stock data with regulatory filings for comprehensive company view
*/

WITH stocks AS (
    SELECT * FROM {{ ref('mart_stock_summary') }}
),

filings AS (
    SELECT * FROM {{ ref('int_filing_activity') }}
),

recent_filings AS (
    SELECT
        ticker,
        form_type,
        filing_date,
        filing_url,
        ROW_NUMBER() OVER (
            PARTITION BY ticker
            ORDER BY filing_date DESC
        ) AS rn
    FROM {{ ref('stg_sec_filings') }}
),

latest_10k AS (
    SELECT ticker, filing_date AS last_10k_date, filing_url AS last_10k_url
    FROM recent_filings
    WHERE form_type = '10-K' AND rn = 1
),

latest_10q AS (
    SELECT ticker, filing_date AS last_10q_date, filing_url AS last_10q_url
    FROM recent_filings
    WHERE form_type = '10-Q' AND rn = 1
),

latest_8k AS (
    SELECT ticker, filing_date AS last_8k_date
    FROM recent_filings
    WHERE form_type = '8-K' AND rn = 1
)

SELECT
    s.symbol,
    f.company_name,

    -- Stock metrics
    s.current_price,
    s.daily_return_pct,
    s.return_5d_pct,
    s.return_20d_pct,
    s.volatility_20,
    s.trend_signal,
    s.volume,
    s.relative_volume,

    -- Filing activity
    f.total_filings,
    f.recent_8k_filings,
    f.insider_transactions,
    f.activity_level,
    f.insider_activity_level,
    f.days_since_filing,

    -- Key filing dates
    k10.last_10k_date,
    k10.last_10k_url,
    q10.last_10q_date,
    q10.last_10q_url,
    k8.last_8k_date,

    -- Days until expected filings
    CASE
        WHEN k10.last_10k_date IS NOT NULL
        THEN 365 - (CURRENT_DATE - k10.last_10k_date)
    END AS days_until_10k,

    CASE
        WHEN q10.last_10q_date IS NOT NULL
        THEN 90 - (CURRENT_DATE - q10.last_10q_date)
    END AS days_until_10q,

    -- Combined intelligence score
    CASE
        WHEN f.activity_level = 'high_activity'
            AND f.insider_activity_level = 'high'
            AND s.relative_volume > 1.5
        THEN 'high_activity_alert'

        WHEN f.recent_8k_filings >= 3
            OR f.insider_transactions >= 5
        THEN 'elevated_activity'

        WHEN s.trend_signal = 'bullish'
            AND s.relative_volume > 1.2
        THEN 'positive_momentum'

        WHEN s.trend_signal = 'bearish'
            AND s.volatility_20 > 30
        THEN 'high_risk'

        ELSE 'normal'
    END AS intelligence_flag

FROM stocks s
LEFT JOIN filings f ON s.symbol = f.ticker
LEFT JOIN latest_10k k10 ON s.symbol = k10.ticker
LEFT JOIN latest_10q q10 ON s.symbol = q10.ticker
LEFT JOIN latest_8k k8 ON s.symbol = k8.ticker
ORDER BY s.symbol
