{{
    config(
        materialized = 'table'
    )
}}

WITH src AS (
    SELECT
        EMPLOYEE_ID,
        ACCESS_ID,
        EMPLOYEE_NAME,
        EMPLOYEE_EMAIL,
        DEPARTMENT_ID,
        OFFICE_ID,
        MANAGER_EMPLOYEE_ID,
        JOB_TITLE,
        JOB_LEVEL,
        EMPLOYMENT_STATUS,
        HIRE_DATE,
        IS_ACTIVE,
        __SRC_DELTA_DAY,
        {{ hash_key(['ACCESS_ID', 'EMPLOYEE_NAME', 'EMPLOYEE_EMAIL', 'DEPARTMENT_ID', 'OFFICE_ID',
                      'MANAGER_EMPLOYEE_ID', 'JOB_TITLE', 'JOB_LEVEL', 'EMPLOYMENT_STATUS',
                      'HIRE_DATE', 'IS_ACTIVE']) }} AS row_hash,
        ROW_NUMBER() OVER (
            PARTITION BY EMPLOYEE_ID
            ORDER BY __SRC_DELTA_DAY ASC NULLS FIRST, EMPLOYEE_ID ASC
        ) AS version_num
    FROM {{ ref('silver_employees') }}
),

deduped AS (
    SELECT *,
        LAG(row_hash) OVER (PARTITION BY EMPLOYEE_ID ORDER BY version_num) AS prev_hash
    FROM src
),

changed AS (
    SELECT * FROM deduped
    WHERE prev_hash IS NULL OR row_hash != prev_hash
),

dim_dept AS (
    SELECT DEPARTMENT_HK, DEPARTMENT_ID FROM {{ ref('dim_department') }} WHERE __IS_CURRENT
),
dim_ofc AS (
    SELECT OFFICE_HK, OFFICE_ID FROM {{ ref('dim_office') }} WHERE __IS_CURRENT
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
                ) OVER (PARTITION BY c.EMPLOYEE_ID ORDER BY c.version_num)
            ),
            '9999-12-31'::DATE
        ) AS effective_to
    FROM changed c
)

SELECT
    {{ hash_key(['s.EMPLOYEE_ID']) }}          AS EMPLOYEE_HK,
    s.EMPLOYEE_ID,
    s.ACCESS_ID,
    s.EMPLOYEE_NAME,
    s.EMPLOYEE_EMAIL,
    d.DEPARTMENT_HK,
    o.OFFICE_HK,
    {{ hash_key(['s.MANAGER_EMPLOYEE_ID']) }}  AS MANAGER_EMPLOYEE_HK,
    s.JOB_TITLE,
    s.JOB_LEVEL,
    s.EMPLOYMENT_STATUS,
    s.HIRE_DATE,
    s.IS_ACTIVE,
    s.effective_to = '9999-12-31'::DATE       AS __IS_CURRENT,
    s.row_hash                                 AS __ROW_HASH,
    s.effective_from                           AS __EFFECTIVE_FROM_DATE,
    s.effective_to                             AS __EFFECTIVE_TO_DATE

FROM scd2 s
LEFT JOIN dim_dept d ON s.DEPARTMENT_ID = d.DEPARTMENT_ID
LEFT JOIN dim_ofc  o ON s.OFFICE_ID = o.OFFICE_ID
