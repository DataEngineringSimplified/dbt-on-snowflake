-- Flags active projects that have a budget but zero staffing assignments.
-- Co-authored with CoCo

-- Every active project with an approved budget should have at least one assignment.
-- Rows returned here are budgeted projects with no staffing.

-- Severity: warn — unstaffed projects are a planning concern, not a data defect.
-- The pipeline should still succeed so downstream models remain fresh.
{{ config(severity='warn') }}

SELECT
    p.PROJECT_ID,
    p.PROJECT_NAME,
    b.PROJECT_BUDGET_USD
FROM {{ ref('fct_project_budget_plan') }} b
JOIN {{ ref('dim_project') }} p
    ON b.PROJECT_HK = p.PROJECT_HK
    AND p.__IS_CURRENT
    AND p.PROJECT_STATUS = 'Active'
WHERE b.__IS_CURRENT
  AND b.PROJECT_BUDGET_USD > 0
  AND NOT EXISTS (
      SELECT 1
      FROM {{ ref('fact_employee_project_assignment') }} a
      WHERE a.PROJECT_HK = b.PROJECT_HK
        AND a.__IS_CURRENT
  )
