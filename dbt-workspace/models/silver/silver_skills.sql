{{
    config(
        materialized = 'table',
        transient = true
    )
}}

SELECT
    CAST(SKILL_ID AS NUMBER)       AS SKILL_ID,
    TRIM(SKILL_NAME)               AS SKILL_NAME,
    TRIM(SKILL_CATEGORY)           AS SKILL_CATEGORY,
    TRY_TO_DATE(ADDED_DATE)       AS ADDED_DATE,
    TRY_TO_DATE(UPDATED_DATE)     AS UPDATED_DATE,
    CAST(IS_ACTIVE AS BOOLEAN)     AS IS_ACTIVE,
    UPPER(REPLACE(TRIM(SKILL_NAME), ' ', '_')) AS SKILL_CODE

FROM {{ source('bronze', 'SKILLS') }}
WHERE SKILL_ID IS NOT NULL
  AND TRIM(SKILL_NAME) != ''
