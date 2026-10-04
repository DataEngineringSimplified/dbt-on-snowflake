{#
  Type: RUN-OPERATION MACRO. Does not override any built-in dbt macro.

  Checks staleness of a BRONZE table by comparing MAX(load timestamp) to now.
  Logs PASS/WARN/ERROR; raises a compiler error if error threshold is breached.

  Params:  database (default DBT_HR_ANALYTICS), schema (BRONZE),
           table_name (EMPLOYEE_DAILY_ACCESS), loaded_at_field (__STG_LOAD_TS),
           warn_after_hours (24), error_after_hours (168)
  Result:  Freshness report logged; halts on ERROR.

  SQL: SELECT MAX(<field>), CURRENT_TIMESTAMP(), DATEDIFF('hour', MAX(<field>), CURRENT_TIMESTAMP())
       FROM <database>.<schema>.<table>

  Usage:
    dbt run-operation check_source_freshness
    dbt run-operation check_source_freshness --args '{table_name: EMPLOYEES, warn_after_hours: 12}'
#}
-- Macro to check source freshness when dbt source freshness is unavailable
-- Co-authored with CoCo

{% macro check_source_freshness(
    database='DBT_HR_ANALYTICS',
    schema='BRONZE',
    table_name='EMPLOYEE_DAILY_ACCESS',
    loaded_at_field='__STG_LOAD_TS',
    warn_after_hours=24,
    error_after_hours=168
) %}

    {% set query %}
        SELECT
            MAX({{ loaded_at_field }}) AS latest_load_ts,
            CURRENT_TIMESTAMP()       AS checked_at,
            DATEDIFF('hour', MAX({{ loaded_at_field }}), CURRENT_TIMESTAMP()) AS hours_since_last_load
        FROM {{ database }}.{{ schema }}.{{ table_name }}
    {% endset %}

    {% set result = run_query(query) %}

    {% if execute %}
        {% set hours_stale = result.columns[2].values()[0] %}
        {% set latest_ts   = result.columns[0].values()[0] %}

        {{ log("────────────────────────────────────────────", info=True) }}
        {{ log("Source freshness check: " ~ database ~ "." ~ schema ~ "." ~ table_name, info=True) }}
        {{ log("  Loaded-at field : " ~ loaded_at_field, info=True) }}
        {{ log("  Latest load     : " ~ latest_ts, info=True) }}
        {{ log("  Hours since load: " ~ hours_stale, info=True) }}
        {{ log("  Warn threshold  : " ~ warn_after_hours ~ " hours", info=True) }}
        {{ log("  Error threshold : " ~ error_after_hours ~ " hours", info=True) }}

        {% if hours_stale >= error_after_hours %}
            {{ log("  Status          : ERROR - data is " ~ hours_stale ~ " hours stale!", info=True) }}
            {{ log("────────────────────────────────────────────", info=True) }}
            {{ exceptions.raise_compiler_error("FRESHNESS ERROR: " ~ table_name ~ " is " ~ hours_stale ~ " hours stale (threshold: " ~ error_after_hours ~ "h)") }}
        {% elif hours_stale >= warn_after_hours %}
            {{ log("  Status          : WARN - data is " ~ hours_stale ~ " hours stale", info=True) }}
            {{ log("────────────────────────────────────────────", info=True) }}
        {% else %}
            {{ log("  Status          : PASS", info=True) }}
            {{ log("────────────────────────────────────────────", info=True) }}
        {% endif %}
    {% endif %}

{% endmacro %}