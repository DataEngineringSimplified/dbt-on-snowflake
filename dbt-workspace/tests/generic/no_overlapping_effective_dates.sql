-- =============================================================================
-- Generic test: no_overlapping_effective_dates
--
-- Reusable test that ensures SCD-2 effective date ranges never overlap for the
-- same entity. Apply it to any model that has effective-dated versioning.
--
-- Parameters:
--   model        — the model under test (injected automatically by dbt)
--   partition_key — column that identifies the entity (e.g., ASSIGNMENT_ID)
--   from_date    — effective-from column  (default: __EFFECTIVE_FROM_DATE)
--   to_date      — effective-to column    (default: __EFFECTIVE_TO_DATE)
--
-- Usage in YAML:
--   models:
--     - name: fact_employee_project_assignment
--       tests:
--         - no_overlapping_effective_dates:
--             partition_key: ASSIGNMENT_ID
--             severity: error
--
-- How it works:
--   For each entity, LEAD() fetches the next version's from_date. If the
--   current to_date >= the next from_date, the ranges overlap — a defect in
--   the SCD pipeline.
--
-- Rows returned = violations. Zero rows = pass.
-- =============================================================================

{% test no_overlapping_effective_dates(model, partition_key, from_date='__EFFECTIVE_FROM_DATE', to_date='__EFFECTIVE_TO_DATE') %}

WITH windowed AS (
    SELECT
        {{ partition_key }},
        {{ from_date }}  AS eff_from,
        {{ to_date }}    AS eff_to,
        LEAD({{ from_date }}) OVER (
            PARTITION BY {{ partition_key }}
            ORDER BY {{ from_date }}
        ) AS next_eff_from
    FROM {{ model }}
)

SELECT
    {{ partition_key }},
    eff_from,
    eff_to,
    next_eff_from
FROM windowed
WHERE next_eff_from IS NOT NULL
  AND eff_to >= next_eff_from   -- ranges overlap

{% endtest %}
