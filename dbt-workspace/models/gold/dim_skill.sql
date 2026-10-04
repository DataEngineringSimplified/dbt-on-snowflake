{{
    config(
        materialized = 'table'
    )
}}

WITH latest_per_skill AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY SKILL_ID ORDER BY UPDATED_DATE DESC NULLS LAST, SKILL_ID) AS rn
    FROM {{ ref('silver_skills') }}
)

SELECT
    {{ hash_key(['SKILL_ID']) }}  AS SKILL_HK,
    SKILL_ID,
    SKILL_NAME,
    SKILL_CATEGORY,
    IS_ACTIVE,
    {{ hash_key(['SKILL_NAME', 'SKILL_CATEGORY', 'IS_ACTIVE']) }} AS __ROW_HASH

FROM latest_per_skill
WHERE rn = 1
