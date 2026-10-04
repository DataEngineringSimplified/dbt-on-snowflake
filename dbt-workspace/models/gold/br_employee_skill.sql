{{
    config(
        materialized = 'view'
    )
}}

WITH src AS (
    SELECT
        EMPLOYEE_SKILL_ID,
        EMPLOYEE_ID,
        SKILL_ID,
        PROFICIENCY_LEVEL,
        IS_PRIMARY_SKILL,
        __SRC_DELTA_DAY,
        {{ hash_key(['PROFICIENCY_LEVEL', 'IS_PRIMARY_SKILL']) }} AS row_hash,
        ROW_NUMBER() OVER (
            PARTITION BY EMPLOYEE_SKILL_ID
            ORDER BY __SRC_DELTA_DAY ASC NULLS FIRST, EMPLOYEE_SKILL_ID
        ) AS version_num
    FROM {{ ref('silver_employee_skills') }}
),

deduped AS (
    SELECT *,
        LAG(row_hash) OVER (PARTITION BY EMPLOYEE_SKILL_ID ORDER BY version_num) AS prev_hash
    FROM src
),

changed AS (
    SELECT * FROM deduped
    WHERE prev_hash IS NULL OR row_hash != prev_hash
),

scd AS (
    SELECT
        *,
        COALESCE(
            TRY_TO_DATE('2026-09-' || LPAD(__SRC_DELTA_DAY, 2, '0')),
            '2020-01-01'::DATE
        ) AS effective_from,
        COALESCE(
            DATEADD(DAY, -1,
                LEAD(
                    COALESCE(
                        TRY_TO_DATE('2026-09-' || LPAD(__SRC_DELTA_DAY, 2, '0')),
                        '2020-01-01'::DATE
                    )
                ) OVER (PARTITION BY EMPLOYEE_SKILL_ID ORDER BY version_num)
            ),
            '9999-12-31'::DATE
        ) AS effective_to
    FROM changed
),

dim_emp AS (
    SELECT EMPLOYEE_HK, EMPLOYEE_ID FROM {{ ref('dim_employee') }} WHERE __IS_CURRENT
),
dim_sk AS (
    SELECT SKILL_HK, SKILL_ID FROM {{ ref('dim_skill') }}
),
dim_prof AS (
    SELECT PROFICIENCY_HK, PROFICIENCY_LEVEL FROM {{ ref('dim_proficiency') }}
)

SELECT
    {{ hash_key(['s.EMPLOYEE_SKILL_ID', 's.effective_from']) }} AS EMPLOYEE_SKILL_VERSION_HK,
    s.EMPLOYEE_SKILL_ID,
    e.EMPLOYEE_HK,
    sk.SKILL_HK,
    pr.PROFICIENCY_HK,
    s.IS_PRIMARY_SKILL,
    s.effective_from                    AS __EFFECTIVE_FROM_DATE,
    s.effective_to                      AS __EFFECTIVE_TO_DATE,
    s.effective_to = '9999-12-31'::DATE AS __IS_CURRENT,
    s.row_hash                          AS __ROW_HASH

FROM scd s
LEFT JOIN dim_emp  e  ON s.EMPLOYEE_ID = e.EMPLOYEE_ID
LEFT JOIN dim_sk   sk ON s.SKILL_ID = sk.SKILL_ID
LEFT JOIN dim_prof pr ON s.PROFICIENCY_LEVEL = pr.PROFICIENCY_LEVEL
