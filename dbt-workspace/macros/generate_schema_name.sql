{#
  Type: BUILT-IN OVERRIDE -- Overwrites dbt's built-in `generate_schema_name`.
  Invoked automatically during compilation, not via dbt run-operation.

  dbt's default prefixes target.schema to custom_schema_name (e.g. "DEV_GOLD").
  This override uses the custom schema name as-is, so `schema: GOLD` compiles
  to GOLD directly -- not DEV_GOLD.

  Params:  custom_schema_name (string|None) -- from model config / dbt_project.yml
           node (object) -- dbt graph node (unused)
  Returns: Schema name string for the compiled SQL reference.

  Example: schema=GOLD on DEV target => DBT_HR_ANALYTICS.GOLD.my_model (not DEV_GOLD)
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
