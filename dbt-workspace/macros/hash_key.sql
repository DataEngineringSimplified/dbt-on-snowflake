{#
  Type: INLINE / SCALAR MACRO (used inside model SQL, not via dbt run-operation).
  Does not override any built-in dbt macro.

  Generates a deterministic SHA-256 surrogate hash key from one or more columns.

  Params:  columns (list) -- e.g. ['EMPLOYEE_ID'] or ['PROJECT_ID', 'UPDATED_DATE']
  Returns: Inline SQL expression producing a 64-char hex hash (VARCHAR).

  SQL output example:
    {{ hash_key(['EMPLOYEE_ID', 'DEPARTMENT_ID']) }}
    =>  SHA2(CONCAT_WS('||',
          COALESCE(CAST(EMPLOYEE_ID AS VARCHAR), '^^NULL^^'),
          COALESCE(CAST(DEPARTMENT_ID AS VARCHAR), '^^NULL^^')), 256)

  NULLs are replaced with '^^NULL^^'; columns joined by '||' to avoid collisions.
#} {% macro hash_key(columns) %} SHA2(
    CONCAT_WS(
        '||',
        {%- for col in columns %} COALESCE(
            CAST({{ col }} AS VARCHAR),
            '^^NULL^^'
        ) {%- if not loop.last %},
        {%- endif %}{%- endfor %}
    ),
    256
) {% endmacro %}