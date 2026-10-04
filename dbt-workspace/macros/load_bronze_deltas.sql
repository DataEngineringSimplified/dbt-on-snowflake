{#
  Type: RUN-OPERATION MACRO. Does not override any built-in dbt macro.

  Batch-loads daily-incremental CSVs (day_01..day_05) into all 10 BRONZE tables.
  Convenience wrapper that loops days x entities and runs COPY INTO for each.

  Params:  None (day list and entities are hardcoded).
  Result:  Incremental rows appended to BRONZE tables; COPY INTO dedup prevents re-loads.

  SQL per day/entity:
    COPY INTO DBT_HR_ANALYTICS.BRONZE.<TABLE>
    FROM @MY_S3_STAGE PATTERN='.*daily-incremental/<day>/.*<file>.*[.]csv' ON_ERROR='CONTINUE'

  Usage: dbt run-operation load_bronze_deltas
#} {% macro load_bronze_deltas() %} {% set days = ['day_01', 'day_02', 'day_03', 'day_04', 'day_05'] %} {% set stage = 'DBT_HR_ANALYTICS.UTIL.MY_S3_STAGE' %} {% set ff    = 'DBT_HR_ANALYTICS.UTIL.CSV_FF' %} {% set entities = [
        {'table': 'DEPARTMENTS',                  'file': '01_departments_master',            'n': 6},
        {'table': 'OFFICES',                      'file': '02_offices_master',                'n': 8},
        {'table': 'COMPANIES',                    'file': '03_companies_master',              'n': 8},
        {'table': 'EMPLOYEES',                    'file': '04_employees_master',              'n': 14},
        {'table': 'PROJECTS',                     'file': '05_projects_master',               'n': 14},
        {'table': 'EMPLOYEE_PROJECT_ASSIGNMENTS', 'file': '06_employee_project_assignments',  'n': 7},
        {'table': 'EMPLOYEE_DAILY_ACCESS',        'file': '07_employee_daily_access',         'n': 7},
        {'table': 'SKILLS',                       'file': '08_skills_master',                 'n': 6},
        {'table': 'EMPLOYEE_SKILLS',              'file': '09_employee_skills',               'n': 5},
        {'table': 'PROJECT_TECHNOLOGIES',         'file': '10_project_technologies',          'n': 5}
    ] %} {% for day in days %} {% for e in entities %} {% set pattern = ".*daily-incremental/" ~ day ~ "/.*" ~ e.file ~ ".*[.]csv" %} {% set select_cols = [] %} {% for i in range(1, e.n + 1) %} {% do select_cols.append("$" ~ i) %} {% endfor %} {% do select_cols.append("METADATA$FILENAME") %} {% do select_cols.append("METADATA$START_SCAN_TIME") %} {% set copy_sql %} COPY INTO DBT_HR_ANALYTICS.BRONZE.{{ e.table }}
FROM
    (
        SELECT
            {{ select_cols | join(', ') }}
        FROM
            @{{ stage }}
    ) FILE_FORMAT = (FORMAT_NAME = '{{ ff }}') PATTERN = '{{ pattern }}' ON_ERROR = 'CONTINUE' {% endset %} {% do run_query(copy_sql) %} {% endfor %} {{ log("Loaded delta " ~ day, info=True) }} {% endfor %} {{ log("All daily deltas loaded.", info=True) }} {% endmacro %}