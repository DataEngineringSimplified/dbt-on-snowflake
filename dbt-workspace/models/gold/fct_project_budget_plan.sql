{{
    config(
        materialized = 'table'
    )
}}

WITH src AS (
    SELECT
        PROJECT_ID,
        PROJECT_BUDGET_USD,
        __SRC_DELTA_DAY,
        {{ hash_key(['PROJECT_BUDGET_USD']) }} AS row_hash,
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
                ) OVER (PARTITION BY PROJECT_ID ORDER BY version_num)
            ),
            '9999-12-31'::DATE
        ) AS effective_to
    FROM changed
),

dim_proj AS (
    SELECT PROJECT_HK, PROJECT_ID, COMPANY_HK, OWNING_DEPARTMENT_HK
    FROM {{ ref('dim_project') }}
    WHERE __IS_CURRENT
)

SELECT
    {{ hash_key(['s.PROJECT_ID', 's.effective_from']) }} AS PROJECT_BUDGET_VERSION_HK,
    s.PROJECT_ID,
    p.PROJECT_HK,
    p.COMPANY_HK,
    p.OWNING_DEPARTMENT_HK,
    s.PROJECT_BUDGET_USD,
    s.effective_from                    AS __EFFECTIVE_FROM_DATE,
    s.effective_to                      AS __EFFECTIVE_TO_DATE,
    s.effective_to = '9999-12-31'::DATE AS __IS_CURRENT,
    s.row_hash                          AS __ROW_HASH

FROM scd s
LEFT JOIN dim_proj p ON s.PROJECT_ID = p.PROJECT_ID
