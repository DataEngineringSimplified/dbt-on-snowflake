{{
    config(
        materialized = 'table',
        transient = true
    )
}}

SELECT
    CAST(e.EMPLOYEE_ID AS NUMBER)              AS EMPLOYEE_ID,
    TRIM(e.ACCESS_ID)                          AS ACCESS_ID,
    TRIM(e.EMPLOYEE_NAME)                      AS EMPLOYEE_NAME,
    LOWER(TRIM(e.EMPLOYEE_EMAIL))              AS EMPLOYEE_EMAIL,
    CAST(e.DEPARTMENT_ID AS NUMBER)            AS DEPARTMENT_ID,
    CAST(e.OFFICE_ID AS NUMBER)                AS OFFICE_ID,
    TRY_CAST(e.MANAGER_EMPLOYEE_ID AS NUMBER)  AS MANAGER_EMPLOYEE_ID,
    TRIM(e.JOB_TITLE)                          AS JOB_TITLE,
    TRIM(e.JOB_LEVEL)                          AS JOB_LEVEL,
    TRIM(e.EMPLOYMENT_STATUS)                  AS EMPLOYMENT_STATUS,
    TRY_TO_DATE(e.HIRE_DATE)                   AS HIRE_DATE,
    TRY_TO_DATE(e.ADDED_DATE)                 AS ADDED_DATE,
    TRY_TO_DATE(e.UPDATED_DATE)               AS UPDATED_DATE,
    CAST(e.IS_ACTIVE AS BOOLEAN)               AS IS_ACTIVE,
    SPLIT_PART(LOWER(TRIM(e.EMPLOYEE_EMAIL)), '@', 2) AS EMAIL_DOMAIN,
    INITCAP(TRIM(e.EMPLOYEE_NAME))             AS EMPLOYEE_NAME_NORMALIZED,
    REGEXP_SUBSTR(e.__STG_FILE_NAME, 'day_(\\d+)', 1, 1, 'e') AS __SRC_DELTA_DAY

FROM {{ source('bronze', 'EMPLOYEES') }} e
WHERE e.EMPLOYEE_ID IS NOT NULL
  AND TRIM(e.EMPLOYEE_NAME) != ''
  AND CAST(e.DEPARTMENT_ID AS NUMBER) IN (SELECT DEPARTMENT_ID FROM {{ ref('silver_departments') }})
  AND CAST(e.OFFICE_ID AS NUMBER) IN (SELECT OFFICE_ID FROM {{ ref('silver_offices') }})
