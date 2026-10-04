{{
    config(
        materialized = 'table'
    )
}}

WITH src AS (
    SELECT
        COMPANY_ID,
        COMPANY_NAME,
        INDUSTRY,
        COMPANY_COUNTRY,
        COMPANY_CLASSIFICATION,
        IS_ACTIVE,
        __SRC_DELTA_DAY,
        {{ hash_key(['COMPANY_NAME', 'INDUSTRY', 'COMPANY_COUNTRY', 'COMPANY_CLASSIFICATION', 'IS_ACTIVE']) }} AS row_hash,
        ROW_NUMBER() OVER (
            PARTITION BY COMPANY_ID
            ORDER BY __SRC_DELTA_DAY ASC NULLS FIRST, COMPANY_ID ASC
        ) AS version_num
    FROM {{ ref('silver_companies') }}
),

deduped AS (
    SELECT *,
        LAG(row_hash) OVER (PARTITION BY COMPANY_ID ORDER BY version_num) AS prev_hash
    FROM src
),

changed AS (
    SELECT * FROM deduped
    WHERE prev_hash IS NULL OR row_hash != prev_hash
),

scd2 AS (
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
                ) OVER (PARTITION BY COMPANY_ID ORDER BY version_num)
            ),
            '9999-12-31'::DATE
        ) AS effective_to
    FROM changed
)

SELECT
    {{ hash_key(['COMPANY_ID']) }} AS COMPANY_HK,
    COMPANY_ID,
    COMPANY_NAME,
    INDUSTRY,
    COMPANY_COUNTRY,
    COMPANY_CLASSIFICATION,
    IS_ACTIVE,
    effective_to = '9999-12-31'::DATE AS __IS_CURRENT,
    row_hash                          AS __ROW_HASH,
    effective_from                    AS __EFFECTIVE_FROM_DATE,
    effective_to                      AS __EFFECTIVE_TO_DATE

FROM scd2
