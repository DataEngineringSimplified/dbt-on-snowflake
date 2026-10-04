{{
    config(
        materialized = 'table',
        transient = true
    )
}}

WITH country_map AS (
    SELECT
        SOURCE_VALUE        AS src,
        COUNTRY_ISO2        AS iso2,
        COUNTRY_ISO3        AS iso3,
        COUNTRY_NAME_STANDARDIZED AS std_name,
        DEFAULT_CURRENCY_CODE     AS currency
    FROM {{ ref('country_mapping') }}
)

SELECT
    CAST(c.COMPANY_ID AS NUMBER)           AS COMPANY_ID,
    TRIM(c.COMPANY_NAME)                   AS COMPANY_NAME,
    TRIM(c.INDUSTRY)                       AS INDUSTRY,
    TRIM(c.COMPANY_COUNTRY)                AS COMPANY_COUNTRY,
    TRIM(c.COMPANY_CLASSIFICATION)         AS COMPANY_CLASSIFICATION,
    TRY_TO_DATE(c.ADDED_DATE)             AS ADDED_DATE,
    TRY_TO_DATE(c.UPDATED_DATE)           AS UPDATED_DATE,
    CAST(c.IS_ACTIVE AS BOOLEAN)           AS IS_ACTIVE,
    cm.iso2                                 AS COUNTRY_ISO2,
    cm.iso3                                 AS COUNTRY_ISO3,
    cm.std_name                             AS COUNTRY_NAME_STANDARDIZED,
    cm.currency                             AS DEFAULT_CURRENCY_CODE,
    REGEXP_SUBSTR(c.__STG_FILE_NAME, 'day_(\\d+)', 1, 1, 'e') AS __SRC_DELTA_DAY

FROM {{ source('bronze', 'COMPANIES') }} c
LEFT JOIN country_map cm ON UPPER(TRIM(c.COMPANY_COUNTRY)) = UPPER(cm.src)
WHERE c.COMPANY_ID IS NOT NULL
  AND TRIM(c.COMPANY_NAME) != ''
  AND cm.src IS NOT NULL
