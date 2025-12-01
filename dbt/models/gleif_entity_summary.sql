{{ config(materialized='table') }}

/*
  GLEIF Entity Summary Model
  Creates a cleaned and enriched view of legal entities with hierarchy information
*/

WITH entities AS (
    SELECT
        lei,
        legal_name,
        legal_form,
        jurisdiction,
        category,
        status,
        creation_date,
        city,
        region,
        country,
        postal_code,
        direct_parent_lei,
        ultimate_parent_lei,
        last_update
    FROM gleif_entities
    WHERE status = 'ACTIVE'
),

parent_lookup AS (
    SELECT
        lei,
        legal_name as parent_name
    FROM gleif_entities
),

enriched AS (
    SELECT
        e.*,
        dp.parent_name as direct_parent_name,
        up.parent_name as ultimate_parent_name,
        CASE 
            WHEN e.direct_parent_lei = '' AND e.ultimate_parent_lei = '' THEN 'Standalone'
            WHEN e.ultimate_parent_lei = e.lei THEN 'Ultimate Parent'
            WHEN e.direct_parent_lei != '' THEN 'Subsidiary'
            ELSE 'Unknown'
        END as hierarchy_level,
        CASE
            WHEN e.country IN ('US', 'USA', 'United States') THEN 'North America'
            WHEN e.country IN ('GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'CH') THEN 'Europe'
            WHEN e.country IN ('JP', 'CN', 'HK', 'SG', 'KR') THEN 'Asia Pacific'
            ELSE 'Other'
        END as region_group
    FROM entities e
    LEFT JOIN parent_lookup dp ON e.direct_parent_lei = dp.lei
    LEFT JOIN parent_lookup up ON e.ultimate_parent_lei = up.lei
)

SELECT * FROM enriched
