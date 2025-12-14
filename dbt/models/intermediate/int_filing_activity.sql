{{
    config(
        materialized='view',
        description='SEC filing activity analysis by company'
    )
}}

/*
    Intermediate model: Filing Activity
    - Aggregates filing counts by company
    - Identifies recent activity
    - Flags companies with unusual filing patterns
*/

WITH filings AS (
    SELECT * FROM {{ ref('stg_sec_filings') }}
),

-- Filing counts by company and type
company_activity AS (
    SELECT
        ticker,
        company_name,
        filing_category,
        COUNT(*) AS filing_count,
        MIN(filing_date) AS first_filing,
        MAX(filing_date) AS latest_filing
    FROM filings
    GROUP BY ticker, company_name, filing_category
),

-- Recent 8-K filings (material events)
recent_events AS (
    SELECT
        ticker,
        COUNT(*) AS recent_8k_count
    FROM filings
    WHERE
        filing_category = 'current_report'
        AND filing_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY ticker
),

-- Insider trading activity
insider_activity AS (
    SELECT
        ticker,
        COUNT(*) AS insider_trade_count,
        MAX(filing_date) AS last_insider_filing
    FROM filings
    WHERE filing_category = 'insider_transaction'
    GROUP BY ticker
),

-- Summary by company
company_summary AS (
    SELECT
        ticker,
        company_name,
        SUM(filing_count) AS total_filings,
        MAX(latest_filing) AS most_recent_filing,
        MIN(first_filing) AS earliest_filing,
        COUNT(DISTINCT filing_category) AS filing_type_diversity
    FROM company_activity
    GROUP BY ticker, company_name
)

SELECT
    cs.ticker,
    cs.company_name,
    cs.total_filings,
    cs.most_recent_filing,
    cs.earliest_filing,
    cs.filing_type_diversity,

    -- Days since last filing
    CURRENT_DATE - cs.most_recent_filing AS days_since_filing,

    -- Recent 8-K activity
    COALESCE(re.recent_8k_count, 0) AS recent_8k_filings,

    -- Insider activity
    COALESCE(ia.insider_trade_count, 0) AS insider_transactions,
    ia.last_insider_filing,

    -- Activity level indicator
    CASE
        WHEN COALESCE(re.recent_8k_count, 0) >= 5 THEN 'high_activity'
        WHEN COALESCE(re.recent_8k_count, 0) >= 2 THEN 'normal_activity'
        ELSE 'low_activity'
    END AS activity_level,

    -- Insider activity flag
    CASE
        WHEN COALESCE(ia.insider_trade_count, 0) >= 10 THEN 'high'
        WHEN COALESCE(ia.insider_trade_count, 0) >= 3 THEN 'moderate'
        ELSE 'low'
    END AS insider_activity_level

FROM company_summary cs
LEFT JOIN recent_events re ON cs.ticker = re.ticker
LEFT JOIN insider_activity ia ON cs.ticker = ia.ticker
ORDER BY cs.total_filings DESC
