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
    CAST(o.OFFICE_ID AS NUMBER)        AS OFFICE_ID,
    TRIM(o.OFFICE_CODE)                AS OFFICE_CODE,
    TRIM(o.OFFICE_CITY)                AS OFFICE_CITY,
    TRIM(o.OFFICE_COUNTRY)             AS OFFICE_COUNTRY,
    TRIM(o.OFFICE_REGION)              AS OFFICE_REGION,
    TRY_TO_DATE(o.ADDED_DATE)         AS ADDED_DATE,
    TRY_TO_DATE(o.UPDATED_DATE)       AS UPDATED_DATE,
    CAST(o.IS_ACTIVE AS BOOLEAN)       AS IS_ACTIVE,
    cm.iso2                             AS COUNTRY_ISO2,
    cm.iso3                             AS COUNTRY_ISO3,
    cm.std_name                         AS COUNTRY_NAME_STANDARDIZED,
    cm.currency                         AS DEFAULT_CURRENCY_CODE,
    o.OFFICE_REGION IN ('APAC','EU','NA') AS IS_REGION_VALID

FROM {{ source('bronze', 'OFFICES') }} o
LEFT JOIN country_map cm ON UPPER(TRIM(o.OFFICE_COUNTRY)) = UPPER(cm.src)
WHERE o.OFFICE_ID IS NOT NULL
  AND cm.src IS NOT NULL
