/*
  Business Rule Validation
  -------------------------
  Validates domain-specific constraints that go beyond schema-level tests.
  These are rules the business owns — they can't be expressed as simple
  not_null or unique tests, but violations indicate real data problems.

    1. Allocation sanity   — employee project allocations should not exceed 100%
    2. Manager self-ref    — no employee should be their own manager
    3. Skill coverage gaps — projects requiring skills with zero qualified employees
    4. Budget consistency  — project budget must be positive for billable projects
    5. Orphan managers     — managers listed on employees who don't exist as employees

  Usage: dbt compile, then run each statement independently.
*/


-- ====================================================================
-- CHECK 1: Over-Allocated Employees
-- Total ALLOCATION_PERCENT across active assignments should not exceed
-- 100% per employee. Over-allocation means resource planning is broken.
-- ====================================================================
SELECT
    'Over-allocated employees'  AS check_name,
    e.EMPLOYEE_ID,
    e.EMPLOYEE_NAME,
    SUM(a.ALLOCATION_PERCENT)   AS total_allocation_pct,
    COUNT(*)                    AS active_assignments,
    LISTAGG(p.PROJECT_NAME, ', ') WITHIN GROUP (ORDER BY p.PROJECT_NAME) AS projects,
    CASE
        WHEN SUM(a.ALLOCATION_PERCENT) <= 100 THEN 'PASS'
        WHEN SUM(a.ALLOCATION_PERCENT) <= 120 THEN 'WARN — slightly over'
        ELSE 'FAIL — ' || SUM(a.ALLOCATION_PERCENT) || '% allocated'
    END AS status
FROM {{ ref('fact_employee_project_assignment') }} a
INNER JOIN {{ ref('dim_employee') }} e
    ON a.EMPLOYEE_ID = e.EMPLOYEE_ID
   AND e.__IS_CURRENT = TRUE
INNER JOIN {{ ref('silver_projects') }} p
    ON a.PROJECT_ID = p.PROJECT_ID
WHERE a.__IS_CURRENT = TRUE
  AND a.ASSIGNMENT_END_DATE IS NULL  -- still active
GROUP BY e.EMPLOYEE_ID, e.EMPLOYEE_NAME
HAVING SUM(a.ALLOCATION_PERCENT) > 100
ORDER BY total_allocation_pct DESC;


-- ====================================================================
-- CHECK 2: Manager Self-Reference
-- An employee whose MANAGER_EMPLOYEE_ID equals their own EMPLOYEE_ID
-- is a data entry error (even CEOs should have NULL, not self-ref).
-- ====================================================================
SELECT
    'Manager self-reference'    AS check_name,
    EMPLOYEE_ID,
    EMPLOYEE_NAME,
    MANAGER_EMPLOYEE_ID,
    'FAIL — employee is own manager' AS status
FROM {{ ref('dim_employee') }}
WHERE __IS_CURRENT = TRUE
  AND MANAGER_EMPLOYEE_ID = EMPLOYEE_ID;


-- ====================================================================
-- CHECK 3: Skill Coverage Gaps
-- Projects that require a skill at a proficiency level, but zero
-- current employees hold that skill at or above the required level.
-- These are staffing blind spots.
-- ====================================================================
WITH required_skills AS (
    SELECT
        pr.PROJECT_ID,
        p.PROJECT_NAME,
        pr.SKILL_ID,
        s.SKILL_NAME,
        pr.REQUIRED_PROFICIENCY_LEVEL
    FROM {{ ref('br_project_skill_requirement') }} pr
    INNER JOIN {{ ref('silver_projects') }} p
        ON pr.PROJECT_ID = p.PROJECT_ID
    INNER JOIN {{ ref('dim_skill') }} ds
        ON pr.SKILL_ID = ds.SKILL_ID
    INNER JOIN {{ ref('silver_skills') }} s
        ON pr.SKILL_ID = s.SKILL_ID
    WHERE pr.__IS_CURRENT = TRUE
),

qualified_employees AS (
    SELECT
        SKILL_ID,
        PROFICIENCY_LEVEL,
        COUNT(DISTINCT EMPLOYEE_ID) AS qualified_count
    FROM {{ ref('br_employee_skill') }}
    WHERE __IS_CURRENT = TRUE
    GROUP BY SKILL_ID, PROFICIENCY_LEVEL
)

SELECT
    'Skill coverage gap'            AS check_name,
    rs.PROJECT_NAME,
    rs.SKILL_NAME,
    rs.REQUIRED_PROFICIENCY_LEVEL   AS required_level,
    COALESCE(qe.qualified_count, 0) AS employees_at_or_above,
    CASE
        WHEN COALESCE(qe.qualified_count, 0) = 0 THEN 'FAIL — no qualified employees'
        WHEN qe.qualified_count < 3              THEN 'WARN — thin coverage'
        ELSE 'PASS'
    END AS status
FROM required_skills rs
LEFT JOIN qualified_employees qe
    ON rs.SKILL_ID = qe.SKILL_ID
   AND qe.PROFICIENCY_LEVEL >= rs.REQUIRED_PROFICIENCY_LEVEL
WHERE COALESCE(qe.qualified_count, 0) < 3
ORDER BY employees_at_or_above ASC, rs.PROJECT_NAME;


-- ====================================================================
-- CHECK 4: Budget Consistency
-- Billable projects (PROJECT_BILLING_TYPE != 'Internal') must have a
-- positive PROJECT_BUDGET_USD. A zero or null budget on a billable
-- project means finance can't track revenue against it.
-- ====================================================================
SELECT
    'Budget consistency'        AS check_name,
    p.PROJECT_ID,
    p.PROJECT_NAME,
    p.PROJECT_BILLING_TYPE,
    b.PROJECT_BUDGET_USD,
    CASE
        WHEN b.PROJECT_BUDGET_USD IS NULL THEN 'FAIL — NULL budget on billable project'
        WHEN b.PROJECT_BUDGET_USD <= 0    THEN 'FAIL — zero/negative budget'
        ELSE 'PASS'
    END AS status
FROM {{ ref('silver_projects') }} p
LEFT JOIN {{ ref('fct_project_budget_plan') }} b
    ON p.PROJECT_ID = b.PROJECT_ID
   AND b.__IS_CURRENT = TRUE
WHERE p.PROJECT_BILLING_TYPE != 'Internal'
  AND (b.PROJECT_BUDGET_USD IS NULL OR b.PROJECT_BUDGET_USD <= 0);


-- ====================================================================
-- CHECK 5: Orphan Managers
-- Employees referencing a MANAGER_EMPLOYEE_ID that doesn't exist in
-- dim_employee. This breaks org-chart reporting and hierarchy queries.
-- ====================================================================
SELECT
    'Orphan manager reference'  AS check_name,
    e.EMPLOYEE_ID,
    e.EMPLOYEE_NAME,
    e.MANAGER_EMPLOYEE_ID       AS missing_manager_id,
    'FAIL — manager not found in dim_employee' AS status
FROM {{ ref('dim_employee') }} e
LEFT JOIN {{ ref('dim_employee') }} m
    ON e.MANAGER_EMPLOYEE_ID = m.EMPLOYEE_ID
   AND m.__IS_CURRENT = TRUE
WHERE e.__IS_CURRENT = TRUE
  AND e.MANAGER_EMPLOYEE_ID IS NOT NULL
  AND m.EMPLOYEE_ID IS NULL;
