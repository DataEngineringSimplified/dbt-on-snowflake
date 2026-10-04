-- =============================================================================
-- Test: assert_no_skill_proficiency_downgrade
-- Layer: Gold (br_employee_skill + dim_proficiency)
--
-- Business rule:
--   An employee's proficiency for a given skill should never decrease over time.
--   Once rated "Advanced", a later effective version should not revert to
--   "Intermediate" or "Beginner". This guards against bad source data or
--   incorrect merge logic in the SCD pipeline.
--
-- How it works:
--   1. Join br_employee_skill to dim_proficiency to obtain a numeric rank.
--   2. Use LAG() partitioned by (EMPLOYEE_HK, SKILL_HK) ordered by effective
--      date to find the previous proficiency rank.
--   3. Return rows where the current rank is strictly less than the prior rank.
--
-- Severity: error
--   A proficiency downgrade indicates a data-quality defect that must be fixed
--   before downstream models (e.g., skill-gap analysis) consume the data.
--
-- Rows returned = violations. Zero rows = pass.
-- =============================================================================

{{ config(severity='error') }}

WITH versioned AS (
    SELECT
        bs.EMPLOYEE_SKILL_VERSION_HK,
        bs.EMPLOYEE_HK,
        bs.SKILL_HK,
        bs.PROFICIENCY_HK,
        bs.__EFFECTIVE_FROM_DATE,
        p.PROFICIENCY_LEVEL,
        p.PROFICIENCY_RANK,
        LAG(p.PROFICIENCY_RANK) OVER (
            PARTITION BY bs.EMPLOYEE_HK, bs.SKILL_HK
            ORDER BY bs.__EFFECTIVE_FROM_DATE
        ) AS PREV_PROFICIENCY_RANK,
        LAG(p.PROFICIENCY_LEVEL) OVER (
            PARTITION BY bs.EMPLOYEE_HK, bs.SKILL_HK
            ORDER BY bs.__EFFECTIVE_FROM_DATE
        ) AS PREV_PROFICIENCY_LEVEL
    FROM {{ ref('br_employee_skill') }} bs
    JOIN {{ ref('dim_proficiency') }} p
        ON bs.PROFICIENCY_HK = p.PROFICIENCY_HK
)

SELECT
    EMPLOYEE_SKILL_VERSION_HK,
    EMPLOYEE_HK,
    SKILL_HK,
    __EFFECTIVE_FROM_DATE,
    PREV_PROFICIENCY_LEVEL  AS FROM_LEVEL,
    PREV_PROFICIENCY_RANK   AS FROM_RANK,
    PROFICIENCY_LEVEL       AS TO_LEVEL,
    PROFICIENCY_RANK        AS TO_RANK
FROM versioned
WHERE PREV_PROFICIENCY_RANK IS NOT NULL          -- skip the first version
  AND PROFICIENCY_RANK < PREV_PROFICIENCY_RANK   -- rank went down = downgrade
