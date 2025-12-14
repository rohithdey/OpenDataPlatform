{{
    config(
        materialized='view',
        description='Cleaned SEC EDGAR filings data'
    )
}}

/*
    Staging model for SEC filings
    - Standardizes ticker symbols
    - Categorizes filing types
    - Adds filing metadata
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'sec_filings') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        UPPER(TRIM(ticker)) AS ticker,
        TRIM(cik) AS cik,
        TRIM(company_name) AS company_name,

        -- Filing info
        UPPER(TRIM(form_type)) AS form_type,
        CAST(filing_date AS DATE) AS filing_date,
        TRIM(accession_number) AS accession_number,
        TRIM(document) AS document_name,
        TRIM(filing_url) AS filing_url,

        -- Filing category
        CASE
            WHEN form_type IN ('10-K', '10-K/A') THEN 'annual_report'
            WHEN form_type IN ('10-Q', '10-Q/A') THEN 'quarterly_report'
            WHEN form_type IN ('8-K', '8-K/A') THEN 'current_report'
            WHEN form_type IN ('DEF 14A', 'DEFA14A') THEN 'proxy_statement'
            WHEN form_type = '4' THEN 'insider_transaction'
            WHEN form_type IN ('S-1', 'S-1/A') THEN 'registration'
            ELSE 'other'
        END AS filing_category,

        -- Is this an amendment?
        CASE
            WHEN form_type LIKE '%/A' THEN TRUE
            ELSE FALSE
        END AS is_amendment,

        -- Metadata
        fetch_timestamp,
        CAST(ingestion_date AS DATE) AS ingestion_date

    FROM source
    WHERE
        ticker IS NOT NULL
        AND form_type IS NOT NULL
        AND filing_date IS NOT NULL
)

SELECT * FROM cleaned
