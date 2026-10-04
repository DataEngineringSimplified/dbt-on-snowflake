-- Flags employees whose current active assignments exceed 100% total allocation.
-- Co-authored with CoCo

-- Severity: warn — over-allocation is a resource-planning issue, not corrupt data.
-- A warning lets the pipeline complete while surfacing the problem in test results.
{{ config(severity='warn') }}

-- An employee's total allocation across current assignments should not exceed 100%.
-- Rows returned here represent overcommitted employees.

SELECT
    e.EMPLOYEE_ID,
    e.EMPLOYEE_NAME,
    SUM(a.ALLOCATION_PERCENT) AS TOTAL_ALLOCATION_PERCENT
FROM {{ ref('fact_employee_project_assignment') }} a
JOIN {{ ref('dim_employee') }} e
    ON a.EMPLOYEE_HK = e.EMPLOYEE_HK
    AND e.__IS_CURRENT
WHERE a.__IS_CURRENT
GROUP BY e.EMPLOYEE_ID, e.EMPLOYEE_NAME
HAVING SUM(a.ALLOCATION_PERCENT) > 100
