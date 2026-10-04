{{
    config(
        materialized = 'table'
    )
}}

SELECT
    {{ hash_key(['proficiency_level']) }} AS PROFICIENCY_HK,
    proficiency_level,
    proficiency_rank
FROM {{ ref('proficiency_levels') }}
