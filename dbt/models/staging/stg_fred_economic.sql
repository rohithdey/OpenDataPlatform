{{
    config(
        materialized='view',
        description='Cleaned FRED economic indicator data'
    )
}}

/*
    Staging model for FRED economic data
    - Standardizes series IDs
    - Handles missing values
    - Adds series metadata
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'fred_economic') }}
),

series_metadata AS (
    -- Define metadata for common series
    SELECT * FROM (VALUES
        ('GDP', 'Gross Domestic Product', 'economic_output', 'quarterly'),
        ('UNRATE', 'Unemployment Rate', 'employment', 'monthly'),
        ('CPIAUCSL', 'Consumer Price Index', 'inflation', 'monthly'),
        ('FEDFUNDS', 'Federal Funds Rate', 'interest_rates', 'daily'),
        ('DGS10', '10-Year Treasury Yield', 'interest_rates', 'daily'),
        ('DGS2', '2-Year Treasury Yield', 'interest_rates', 'daily'),
        ('MORTGAGE30US', '30-Year Mortgage Rate', 'interest_rates', 'weekly'),
        ('HOUST', 'Housing Starts', 'housing', 'monthly'),
        ('UMCSENT', 'Consumer Sentiment', 'sentiment', 'monthly'),
        ('INDPRO', 'Industrial Production', 'economic_output', 'monthly')
    ) AS t(series_id, series_name, category, frequency)
),

cleaned AS (
    SELECT
        -- Identifiers
        UPPER(TRIM(s.series_id)) AS series_id,
        COALESCE(m.series_name, s.series_id) AS series_name,
        COALESCE(m.category, 'other') AS category,
        COALESCE(m.frequency, 'unknown') AS frequency,

        -- Date and value
        CAST(s.date AS DATE) AS observation_date,
        ROUND(s.value, 4) AS value,

        -- Metadata
        s.fetch_timestamp,
        CAST(s.ingestion_date AS DATE) AS ingestion_date

    FROM source s
    LEFT JOIN series_metadata m ON UPPER(TRIM(s.series_id)) = m.series_id
    WHERE
        s.series_id IS NOT NULL
        AND s.date IS NOT NULL
        AND s.value IS NOT NULL
)

SELECT * FROM cleaned
