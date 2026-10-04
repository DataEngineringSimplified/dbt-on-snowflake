/*
  SCD Type 2 Integrity Checks
  ----------------------------
  Validates that the SCD2 implementation in gold dimension tables is correct:
    1. Exactly one current record per business key
    2. No gaps in version history (valid_from/valid_to continuity)
    3. No overlapping effective date ranges
    4. Version numbers are sequential with no duplicates

  Usage: dbt compile, then run each statement independently.
*/


-- ====================================================================
-- CHECK 1: Duplicate Current Records
-- Each business key must have exactly one row where __IS_CURRENT = TRUE.
-- Duplicates mean the SCD2 merge logic has a bug.
-- ====================================================================
{% set scd2_models = [
    {'model': 'dim_employee',   'bk': 'EMPLOYEE_ID'},
    {'model': 'dim_department', 'bk': 'DEPARTMENT_ID'},
    {'model': 'dim_office',     'bk': 'OFFICE_ID'},
    {'model': 'dim_company',    'bk': 'COMPANY_ID'},
    {'model': 'fact_employee_project_assignment', 'bk': 'ASSIGNMENT_ID'},
    {'model': 'br_employee_skill',               'bk': 'EMPLOYEE_SKILL_ID'},
    {'model': 'br_project_skill_requirement',    'bk': 'PROJECT_TECHNOLOGY_ID'}
] %}

{% for m in scd2_models %}
SELECT
    '{{ m.model }}'   AS model_name,
    {{ m.bk }}        AS business_key,
    COUNT(*)          AS current_row_count,
    CASE
        WHEN COUNT(*) = 1 THEN 'PASS'
        ELSE 'FAIL — ' || COUNT(*) || ' current rows'
    END AS status
FROM {{ ref(m.model) }}
WHERE __IS_CURRENT = TRUE
GROUP BY {{ m.bk }}
HAVING COUNT(*) > 1
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %};


-- ====================================================================
-- CHECK 2: Version Gap Detection
-- For each business key, valid_to of version N should equal valid_from
-- of version N+1. A gap means history was lost; an overlap means the
-- close-out step in the merge is broken.
-- ====================================================================
{% for m in scd2_models %}
WITH versioned_{{ loop.index }} AS (
    SELECT
        {{ m.bk }},
        VERSION_NUM,
        __VALID_FROM,
        __VALID_TO,
        LEAD(__VALID_FROM) OVER (
            PARTITION BY {{ m.bk }} ORDER BY VERSION_NUM
        ) AS next_valid_from
    FROM {{ ref(m.model) }}
)

SELECT
    '{{ m.model }}'           AS model_name,
    {{ m.bk }}                AS business_key,
    VERSION_NUM,
    __VALID_TO                AS current_valid_to,
    next_valid_from,
    CASE
        WHEN __VALID_TO = next_valid_from THEN 'PASS'
        WHEN next_valid_from IS NULL      THEN 'OK — latest version'
        WHEN __VALID_TO < next_valid_from THEN 'FAIL — gap detected'
        WHEN __VALID_TO > next_valid_from THEN 'FAIL — overlap detected'
    END AS status
FROM versioned_{{ loop.index }}
WHERE next_valid_from IS NOT NULL
  AND __VALID_TO != next_valid_from
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %};


-- ====================================================================
-- CHECK 3: Duplicate Version Numbers
-- VERSION_NUM must be unique per business key. Duplicates indicate
-- a broken sequence in the SCD2 merge.
-- ====================================================================
{% for m in scd2_models %}
SELECT
    '{{ m.model }}'   AS model_name,
    {{ m.bk }}        AS business_key,
    VERSION_NUM,
    COUNT(*)          AS occurrences
FROM {{ ref(m.model) }}
GROUP BY {{ m.bk }}, VERSION_NUM
HAVING COUNT(*) > 1
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %};


-- ====================================================================
-- CHECK 4: Current Record Has Open-Ended Valid_To
-- The current row (__IS_CURRENT = TRUE) should have __VALID_TO = NULL
-- or a far-future sentinel (e.g. 9999-12-31). A closed date on a
-- current record means the row was expired but never replaced.
-- ====================================================================
{% for m in scd2_models %}
SELECT
    '{{ m.model }}'   AS model_name,
    {{ m.bk }}        AS business_key,
    __VALID_FROM,
    __VALID_TO,
    'FAIL — current record has closed valid_to' AS status
FROM {{ ref(m.model) }}
WHERE __IS_CURRENT = TRUE
  AND __VALID_TO IS NOT NULL
  AND __VALID_TO < '9999-12-31'
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %};
