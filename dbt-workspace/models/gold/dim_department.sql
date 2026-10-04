{{
    config(
        materialized = 'table'
    )
}}

WITH versioned AS (
    SELECT
        DEPARTMENT_ID,
        DEPARTMENT_CODE,
        DEPARTMENT_NAME,
        IS_ACTIVE,
        {{ hash_key(['DEPARTMENT_CODE', 'DEPARTMENT_NAME', 'IS_ACTIVE']) }} AS row_hash,
        ROW_NUMBER() OVER (PARTITION BY DEPARTMENT_ID ORDER BY DEPARTMENT_ID) AS version_num
    FROM {{ ref('silver_departments') }}
),

deduped AS (
    SELECT *,
        LAG(row_hash) OVER (PARTITION BY DEPARTMENT_ID ORDER BY version_num) AS prev_hash
    FROM versioned
),

changed AS (
    SELECT * FROM deduped
    WHERE prev_hash IS NULL OR row_hash != prev_hash
),

scd2 AS (
    SELECT
        *,
        '2020-01-01'::DATE AS effective_from,
        COALESCE(
            DATEADD(DAY, -1,
                LEAD('2020-01-01'::DATE) OVER (PARTITION BY DEPARTMENT_ID ORDER BY version_num)
            ),
            '9999-12-31'::DATE
        ) AS effective_to
    FROM changed
)

SELECT
    {{ hash_key(['DEPARTMENT_ID']) }} AS DEPARTMENT_HK,
    DEPARTMENT_ID,
    DEPARTMENT_CODE,
    DEPARTMENT_NAME,
    IS_ACTIVE,
    effective_to = '9999-12-31'::DATE AS __IS_CURRENT,
    row_hash                          AS __ROW_HASH,
    effective_from                    AS __EFFECTIVE_FROM_DATE,
    effective_to                      AS __EFFECTIVE_TO_DATE

FROM scd2
