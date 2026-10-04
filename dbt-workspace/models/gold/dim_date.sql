{{
    config(
        materialized = 'table'
    )
}}

WITH date_spine AS (
    SELECT DATEADD(DAY, seq4(), '2024-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 3660))
)

SELECT
    {{ hash_key(['date_day']) }} AS DATE_HK,
    date_day                      AS DATE_DAY,
    DAYOFWEEKISO(date_day)        AS DAY_OF_WEEK,
    DAYNAME(date_day)             AS DAY_NAME,
    DAY(date_day)                 AS DAY_OF_MONTH,
    WEEKOFYEAR(date_day)          AS WEEK_OF_YEAR,
    MONTH(date_day)               AS MONTH_NUM,
    MONTHNAME(date_day)           AS MONTH_NAME,
    QUARTER(date_day)             AS QUARTER_NUM,
    YEAR(date_day)                AS YEAR_NUM,
    DAYOFWEEKISO(date_day) <= 5   AS IS_BUSINESS_DAY

FROM date_spine
WHERE date_day <= '2034-01-01'::DATE
