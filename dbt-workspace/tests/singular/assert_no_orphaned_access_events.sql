-- Flags badge access events that cannot be joined to any current employee.
-- Co-authored with CoCo

-- Every access event should resolve to a known employee via EMPLOYEE_HK.
-- Rows returned here are orphaned events with no matching employee dimension record.

-- Severity: error — orphaned foreign keys break referential integrity and
-- will cause NULLs in joined reports.
{{ config(severity='error') }}

SELECT
    f.ACCESS_EVENT_ID,
    f.ACCESS_EVENT_HK,
    f.EMPLOYEE_HK
FROM {{ ref('fact_employee_daily_access') }} f
LEFT JOIN {{ ref('dim_employee') }} e
    ON f.EMPLOYEE_HK = e.EMPLOYEE_HK
WHERE e.EMPLOYEE_HK IS NULL
