-- Flags assignments where end date is before the start date.
-- Co-authored with CoCo

-- Assignment end dates must not precede start dates.
-- Rows returned here have an invalid date sequence.

-- Severity: error — an inverted date range is a hard data defect that will
-- produce negative durations and corrupt downstream metrics.
{{ config(severity='error') }}

SELECT
    a.ASSIGNMENT_ID,
    a.ASSIGNMENT_VERSION_HK,
    a.ASSIGNMENT_START_DATE,
    a.ASSIGNMENT_END_DATE
FROM {{ ref('fact_employee_project_assignment') }} a
WHERE a.ASSIGNMENT_END_DATE < a.ASSIGNMENT_START_DATE
