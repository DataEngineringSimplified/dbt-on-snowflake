/*
  Silver-to-Gold Data Reconciliation
  -----------------------------------
  Validates data consistency between the silver (staging) and gold (dimensional)
  layers across three checks:
    1. Row count comparison (current gold rows vs silver source)
    2. Referential integrity (orphan foreign keys in fact tables)
    3. Value drift (attribute mismatches between silver and gold)

  Usage: dbt compile, then run the compiled SQL from target/compiled/.
*/


-- ====================================================================
-- CHECK 1: Row Count Reconciliation
-- Compares silver row counts against current gold dimension/fact counts.
-- A large delta signals missing loads or broken incremental logic.
-- ====================================================================
WITH row_counts AS (

    SELECT 'silver_employees'              AS source_model,
           'dim_employee (current)'        AS target_model,
           (SELECT COUNT(*) FROM {{ ref('silver_employees') }})           AS silver_rows,
           (SELECT COUNT(*) FROM {{ ref('dim_employee') }} WHERE __IS_CURRENT = TRUE) AS gold_rows

    UNION ALL

    SELECT 'silver_departments',
           'dim_department (current)',
           (SELECT COUNT(*) FROM {{ ref('silver_departments') }}),
           (SELECT COUNT(*) FROM {{ ref('dim_department') }} WHERE __IS_CURRENT = TRUE)

    UNION ALL

    SELECT 'silver_offices',
           'dim_office (current)',
           (SELECT COUNT(*) FROM {{ ref('silver_offices') }}),
           (SELECT COUNT(*) FROM {{ ref('dim_office') }} WHERE __IS_CURRENT = TRUE)

    UNION ALL

    SELECT 'silver_employee_project_assignments',
           'fact_employee_project_assignment (current)',
           (SELECT COUNT(*) FROM {{ ref('silver_employee_project_assignments') }}),
           (SELECT COUNT(*) FROM {{ ref('fact_employee_project_assignment') }} WHERE __IS_CURRENT = TRUE)

    UNION ALL

    SELECT 'silver_employee_skills',
           'br_employee_skill (current)',
           (SELECT COUNT(*) FROM {{ ref('silver_employee_skills') }}),
           (SELECT COUNT(*) FROM {{ ref('br_employee_skill') }} WHERE __IS_CURRENT = TRUE)
)

SELECT
    source_model,
    target_model,
    silver_rows,
    gold_rows,
    silver_rows - gold_rows                                         AS row_difference,
    ROUND((silver_rows - gold_rows) * 100.0 / NULLIF(silver_rows, 0), 2) AS pct_difference,
    CASE
        WHEN silver_rows = gold_rows THEN 'PASS'
        WHEN ABS(silver_rows - gold_rows) <= 5 THEN 'WARN — minor delta'
        ELSE 'FAIL — investigate'
    END AS status
FROM row_counts
ORDER BY ABS(row_difference) DESC;


-- ====================================================================
-- CHECK 2: Referential Integrity
-- Finds orphan foreign keys in fact/bridge tables that have no matching
-- dimension record. Orphans indicate join failures or late-arriving dims.
-- ====================================================================
WITH orphan_checks AS (

    -- Fact assignments referencing missing employees
    SELECT
        'fact_employee_project_assignment' AS fact_table,
        'dim_employee'                     AS dim_table,
        'EMPLOYEE_ID'                      AS fk_column,
        f.EMPLOYEE_ID                      AS orphan_key
    FROM {{ ref('fact_employee_project_assignment') }} f
    LEFT JOIN {{ ref('dim_employee') }} d
        ON f.EMPLOYEE_ID = d.EMPLOYEE_ID
       AND d.__IS_CURRENT = TRUE
    WHERE d.EMPLOYEE_ID IS NULL
      AND f.__IS_CURRENT = TRUE

    UNION ALL

    -- Bridge employee_skill referencing missing employees
    SELECT
        'br_employee_skill',
        'dim_employee',
        'EMPLOYEE_ID',
        b.EMPLOYEE_ID
    FROM {{ ref('br_employee_skill') }} b
    LEFT JOIN {{ ref('dim_employee') }} d
        ON b.EMPLOYEE_ID = d.EMPLOYEE_ID
       AND d.__IS_CURRENT = TRUE
    WHERE d.EMPLOYEE_ID IS NULL
      AND b.__IS_CURRENT = TRUE

    UNION ALL

    -- Bridge employee_skill referencing missing skills
    SELECT
        'br_employee_skill',
        'dim_skill',
        'SKILL_ID',
        b.SKILL_ID
    FROM {{ ref('br_employee_skill') }} b
    LEFT JOIN {{ ref('dim_skill') }} d
        ON b.SKILL_ID = d.SKILL_ID
    WHERE d.SKILL_ID IS NULL
      AND b.__IS_CURRENT = TRUE

    UNION ALL

    -- Bridge project_skill_requirement referencing missing skills
    SELECT
        'br_project_skill_requirement',
        'dim_skill',
        'SKILL_ID',
        br.SKILL_ID
    FROM {{ ref('br_project_skill_requirement') }} br
    LEFT JOIN {{ ref('dim_skill') }} d
        ON br.SKILL_ID = d.SKILL_ID
    WHERE d.SKILL_ID IS NULL
      AND br.__IS_CURRENT = TRUE
)

SELECT
    fact_table,
    dim_table,
    fk_column,
    COUNT(*)                AS orphan_count,
    LISTAGG(DISTINCT orphan_key, ', ') WITHIN GROUP (ORDER BY orphan_key) AS sample_orphan_keys,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL — ' || COUNT(*) || ' orphan(s)'
    END AS status
FROM orphan_checks
GROUP BY fact_table, dim_table, fk_column
ORDER BY orphan_count DESC;


-- ====================================================================
-- CHECK 3: Value Drift Detection
-- Compares attribute values between silver source and current gold
-- dimension rows. Flags records where the gold layer is stale or
-- the transformation produced unexpected values.
-- ====================================================================

-- 3a: Employee attribute drift
SELECT
    'dim_employee'          AS model,
    s.EMPLOYEE_ID,
    'EMPLOYEE_NAME'         AS attribute,
    s.EMPLOYEE_NAME         AS silver_value,
    g.EMPLOYEE_NAME         AS gold_value
FROM {{ ref('silver_employees') }} s
INNER JOIN {{ ref('dim_employee') }} g
    ON s.EMPLOYEE_ID = g.EMPLOYEE_ID
   AND g.__IS_CURRENT = TRUE
WHERE s.EMPLOYEE_NAME != g.EMPLOYEE_NAME

UNION ALL

SELECT
    'dim_employee',
    s.EMPLOYEE_ID,
    'DEPARTMENT_ID',
    s.DEPARTMENT_ID::VARCHAR,
    g.DEPARTMENT_ID::VARCHAR
FROM {{ ref('silver_employees') }} s
INNER JOIN {{ ref('dim_employee') }} g
    ON s.EMPLOYEE_ID = g.EMPLOYEE_ID
   AND g.__IS_CURRENT = TRUE
WHERE s.DEPARTMENT_ID != g.DEPARTMENT_ID

UNION ALL

-- 3b: Office attribute drift
SELECT
    'dim_office',
    s.OFFICE_ID,
    'OFFICE_CITY',
    s.OFFICE_CITY,
    g.OFFICE_CITY
FROM {{ ref('silver_offices') }} s
INNER JOIN {{ ref('dim_office') }} g
    ON s.OFFICE_ID = g.OFFICE_ID
   AND g.__IS_CURRENT = TRUE
WHERE s.OFFICE_CITY != g.OFFICE_CITY

ORDER BY model, attribute;
