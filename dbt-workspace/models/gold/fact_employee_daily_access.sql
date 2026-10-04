{{
    config(
        materialized = 'table'
    )
}}

WITH dim_emp AS (
    SELECT EMPLOYEE_HK, ACCESS_ID
    FROM {{ ref('dim_employee') }}
    WHERE __IS_CURRENT
),

dim_ofc AS (
    SELECT OFFICE_HK, OFFICE_ID
    FROM {{ ref('dim_office') }}
    WHERE __IS_CURRENT
)

SELECT
    {{ hash_key(['da.ACCESS_EVENT_ID']) }} AS ACCESS_EVENT_HK,
    da.ACCESS_EVENT_ID,
    e.EMPLOYEE_HK,
    o.OFFICE_HK,
    da.ACCESS_DATE,
    da.ACCESS_TIMESTAMP,
    da.ACCESS_EVENT_TYPE

FROM {{ ref('silver_employee_daily_access') }} da
LEFT JOIN dim_emp e ON da.ACCESS_ID = e.ACCESS_ID
LEFT JOIN dim_ofc o ON da.OFFICE_ID = o.OFFICE_ID
