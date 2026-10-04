{{
    config(
        materialized = 'table'
    )
}}

WITH src AS (
    SELECT
        ASSIGNMENT_ID,
        EMPLOYEE_ID,
        PROJECT_ID,
        ASSIGNMENT_ROLE,
        ALLOCATION_PERCENT,
        ASSIGNMENT_START_DATE,
        ASSIGNMENT_END_DATE,
        __SRC_DELTA_DAY,
        {{ hash_key(['EMPLOYEE_ID', 'PROJECT_ID', 'ASSIGNMENT_ROLE', 'ALLOCATION_PERCENT',
                      'ASSIGNMENT_START_DATE', 'ASSIGNMENT_END_DATE']) }} AS row_hash,
        ROW_NUMBER() OVER (
            PARTITION BY ASSIGNMENT_ID
            ORDER BY __SRC_DELTA_DAY ASC NULLS FIRST, ASSIGNMENT_ID
        ) AS version_num
    FROM {{ ref('silver_employee_project_assignments') }}
),

deduped AS (
    SELECT *,
        LAG(row_hash) OVER (PARTITION BY ASSIGNMENT_ID ORDER BY version_num) AS prev_hash
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
                ) OVER (PARTITION BY ASSIGNMENT_ID ORDER BY version_num)
            ),
            '9999-12-31'::DATE
        ) AS effective_to
    FROM changed
),

dim_emp AS (
    SELECT EMPLOYEE_HK, EMPLOYEE_ID FROM {{ ref('dim_employee') }} WHERE __IS_CURRENT
),
dim_proj AS (
    SELECT PROJECT_HK, PROJECT_ID FROM {{ ref('dim_project') }} WHERE __IS_CURRENT
),
dim_role AS (
    SELECT ASSIGNMENT_ROLE_HK, ASSIGNMENT_ROLE FROM {{ ref('dim_assignment_role') }}
)

SELECT
    {{ hash_key(['s.ASSIGNMENT_ID', 's.effective_from']) }} AS ASSIGNMENT_VERSION_HK,
    s.ASSIGNMENT_ID,
    e.EMPLOYEE_HK,
    p.PROJECT_HK,
    r.ASSIGNMENT_ROLE_HK,
    s.ALLOCATION_PERCENT,
    s.ASSIGNMENT_START_DATE,
    s.ASSIGNMENT_END_DATE,
    s.effective_from                    AS __EFFECTIVE_FROM_DATE,
    s.effective_to                      AS __EFFECTIVE_TO_DATE,
    s.effective_to = '9999-12-31'::DATE AS __IS_CURRENT,
    s.row_hash                          AS __ROW_HASH

FROM scd s
LEFT JOIN dim_emp  e ON s.EMPLOYEE_ID = e.EMPLOYEE_ID
LEFT JOIN dim_proj p ON s.PROJECT_ID = p.PROJECT_ID
LEFT JOIN dim_role r ON s.ASSIGNMENT_ROLE = r.ASSIGNMENT_ROLE
