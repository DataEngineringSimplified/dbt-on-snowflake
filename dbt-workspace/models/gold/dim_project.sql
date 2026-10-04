{{
    config(
        materialized = 'table'
    )
}}

WITH src AS (
    SELECT
        PROJECT_ID,
        PROJECT_NAME,
        COMPANY_ID,
        OWNING_DEPARTMENT_ID,
        PROJECT_TYPE,
        PROJECT_BILLING_TYPE,
        PROJECT_STATUS,
        START_DATE,
        PLANNED_END_DATE,
        ACTUAL_END_DATE,
        IS_ACTIVE,
        __SRC_DELTA_DAY,
        {{ hash_key(['PROJECT_NAME', 'COMPANY_ID', 'OWNING_DEPARTMENT_ID', 'PROJECT_TYPE',
                      'PROJECT_BILLING_TYPE', 'PROJECT_STATUS', 'START_DATE',
                      'PLANNED_END_DATE', 'ACTUAL_END_DATE', 'IS_ACTIVE']) }} AS row_hash,
        ROW_NUMBER() OVER (
            PARTITION BY PROJECT_ID
            ORDER BY __SRC_DELTA_DAY ASC NULLS FIRST, PROJECT_ID ASC
        ) AS version_num
    FROM {{ ref('silver_projects') }}
),

deduped AS (
    SELECT *,
        LAG(row_hash) OVER (PARTITION BY PROJECT_ID ORDER BY version_num) AS prev_hash
    FROM src
),

changed AS (
    SELECT * FROM deduped
    WHERE prev_hash IS NULL OR row_hash != prev_hash
),

dim_comp AS (
    SELECT COMPANY_HK, COMPANY_ID FROM {{ ref('dim_company') }} WHERE __IS_CURRENT
),
dim_dept AS (
    SELECT DEPARTMENT_HK, DEPARTMENT_ID FROM {{ ref('dim_department') }} WHERE __IS_CURRENT
),

scd2 AS (
    SELECT
        c.*,
        COALESCE(
            TRY_TO_DATE('2026-09-' || LPAD(c.__SRC_DELTA_DAY, 2, '0')),
            '2020-01-01'::DATE
        ) AS effective_from,
        COALESCE(
            DATEADD(DAY, -1,
                LEAD(
                    COALESCE(
                        TRY_TO_DATE('2026-09-' || LPAD(c.__SRC_DELTA_DAY, 2, '0')),
                        '2020-01-01'::DATE
                    )
                ) OVER (PARTITION BY c.PROJECT_ID ORDER BY c.version_num)
            ),
            '9999-12-31'::DATE
        ) AS effective_to
    FROM changed c
)

SELECT
    {{ hash_key(['s.PROJECT_ID']) }}  AS PROJECT_HK,
    s.PROJECT_ID,
    s.PROJECT_NAME,
    comp.COMPANY_HK,
    dept.DEPARTMENT_HK               AS OWNING_DEPARTMENT_HK,
    s.PROJECT_TYPE,
    s.PROJECT_BILLING_TYPE,
    s.PROJECT_STATUS,
    s.START_DATE,
    s.PLANNED_END_DATE,
    s.ACTUAL_END_DATE,
    s.IS_ACTIVE,
    s.effective_to = '9999-12-31'::DATE AS __IS_CURRENT,
    s.row_hash                          AS __ROW_HASH,
    s.effective_from                    AS __EFFECTIVE_FROM_DATE,
    s.effective_to                      AS __EFFECTIVE_TO_DATE

FROM scd2 s
LEFT JOIN dim_comp comp ON s.COMPANY_ID = comp.COMPANY_ID
LEFT JOIN dim_dept dept ON s.OWNING_DEPARTMENT_ID = dept.DEPARTMENT_ID
