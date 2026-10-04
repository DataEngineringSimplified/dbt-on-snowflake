{#
  Type: RUN-OPERATION MACRO. Does not override any built-in dbt macro.

  Loads CSV files from S3 stage into 10 BRONZE tables. Creates tables if missing,
  then runs COPY INTO. Supports base load or day-specific delta load.

  Params:  day_folder (string, optional) -- omit for base load; pass e.g. 'day_06' for delta
  Result:  All BRONZE tables populated with CSV data + __STG_FILE_NAME/__STG_LOAD_TS metadata.

  SQL per entity:
    CREATE TABLE IF NOT EXISTS DBT_HR_ANALYTICS.BRONZE.<TABLE> (...)
    COPY INTO ... FROM @MY_S3_STAGE PATTERN='<regex>' ON_ERROR='CONTINUE'

  Usage:
    dbt run-operation load_bronze
    dbt run-operation load_bronze --args '{day_folder: day_06}'
#}
{% macro load_bronze(day_folder=none) %}

    {% set stage = 'DBT_HR_ANALYTICS.UTIL.MY_S3_STAGE' %}
    {% set ff    = 'DBT_HR_ANALYTICS.UTIL.CSV_FF' %}

    {# Actual CSV column structures discovered from stage files #}
    {% set entities = [
        {
            'table': 'DEPARTMENTS',
            'file': '01_departments_master',
            'n': 6,
            'cols': 'DEPARTMENT_ID VARCHAR, DEPARTMENT_CODE VARCHAR, DEPARTMENT_NAME VARCHAR, ADDED_DATE VARCHAR, UPDATED_DATE VARCHAR, IS_ACTIVE VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        },
        {
            'table': 'OFFICES',
            'file': '02_offices_master',
            'n': 8,
            'cols': 'OFFICE_ID VARCHAR, OFFICE_CODE VARCHAR, OFFICE_CITY VARCHAR, OFFICE_COUNTRY VARCHAR, OFFICE_REGION VARCHAR, ADDED_DATE VARCHAR, UPDATED_DATE VARCHAR, IS_ACTIVE VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        },
        {
            'table': 'COMPANIES',
            'file': '03_companies_master',
            'n': 8,
            'cols': 'COMPANY_ID VARCHAR, COMPANY_NAME VARCHAR, INDUSTRY VARCHAR, COMPANY_COUNTRY VARCHAR, COMPANY_CLASSIFICATION VARCHAR, ADDED_DATE VARCHAR, UPDATED_DATE VARCHAR, IS_ACTIVE VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        },
        {
            'table': 'EMPLOYEES',
            'file': '04_employees_master',
            'n': 14,
            'cols': 'EMPLOYEE_ID VARCHAR, ACCESS_ID VARCHAR, EMPLOYEE_NAME VARCHAR, EMPLOYEE_EMAIL VARCHAR, DEPARTMENT_ID VARCHAR, OFFICE_ID VARCHAR, MANAGER_EMPLOYEE_ID VARCHAR, JOB_TITLE VARCHAR, JOB_LEVEL VARCHAR, EMPLOYMENT_STATUS VARCHAR, HIRE_DATE VARCHAR, ADDED_DATE VARCHAR, UPDATED_DATE VARCHAR, IS_ACTIVE VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        },
        {
            'table': 'PROJECTS',
            'file': '05_projects_master',
            'n': 14,
            'cols': 'PROJECT_ID VARCHAR, PROJECT_NAME VARCHAR, COMPANY_ID VARCHAR, OWNING_DEPARTMENT_ID VARCHAR, PROJECT_TYPE VARCHAR, PROJECT_BILLING_TYPE VARCHAR, PROJECT_BUDGET_USD VARCHAR, PROJECT_STATUS VARCHAR, START_DATE VARCHAR, PLANNED_END_DATE VARCHAR, ACTUAL_END_DATE VARCHAR, ADDED_DATE VARCHAR, UPDATED_DATE VARCHAR, IS_ACTIVE VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        },
        {
            'table': 'EMPLOYEE_PROJECT_ASSIGNMENTS',
            'file': '06_employee_project_assignments',
            'n': 7,
            'cols': 'ASSIGNMENT_ID VARCHAR, EMPLOYEE_ID VARCHAR, PROJECT_ID VARCHAR, ASSIGNMENT_ROLE VARCHAR, ALLOCATION_PERCENT VARCHAR, ASSIGNMENT_START_DATE VARCHAR, ASSIGNMENT_END_DATE VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        },
        {
            'table': 'EMPLOYEE_DAILY_ACCESS',
            'file': '07_employee_daily_access',
            'n': 7,
            'cols': 'ACCESS_EVENT_ID VARCHAR, ACCESS_ID VARCHAR, OFFICE_ID VARCHAR, ACCESS_DATE VARCHAR, ACCESS_TIMESTAMP VARCHAR, ACCESS_EVENT_TYPE VARCHAR, OFFICE_CITY VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        },
        {
            'table': 'SKILLS',
            'file': '08_skills_master',
            'n': 6,
            'cols': 'SKILL_ID VARCHAR, SKILL_NAME VARCHAR, SKILL_CATEGORY VARCHAR, ADDED_DATE VARCHAR, UPDATED_DATE VARCHAR, IS_ACTIVE VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        },
        {
            'table': 'EMPLOYEE_SKILLS',
            'file': '09_employee_skills',
            'n': 5,
            'cols': 'EMPLOYEE_SKILL_ID VARCHAR, EMPLOYEE_ID VARCHAR, SKILL_ID VARCHAR, PROFICIENCY_LEVEL VARCHAR, IS_PRIMARY_SKILL VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        },
        {
            'table': 'PROJECT_TECHNOLOGIES',
            'file': '10_project_technologies',
            'n': 5,
            'cols': 'PROJECT_TECHNOLOGY_ID VARCHAR, PROJECT_ID VARCHAR, SKILL_ID VARCHAR, REQUIRED_PROFICIENCY_LEVEL VARCHAR, IS_PRIMARY_TECHNOLOGY VARCHAR, __STG_FILE_NAME VARCHAR, __STG_LOAD_TS TIMESTAMP_NTZ'
        }
    ] %}

    {% for e in entities %}

        {% set create_sql %}
            CREATE TABLE IF NOT EXISTS DBT_HR_ANALYTICS.BRONZE.{{ e.table }} (
                {{ e.cols }}
            )
        {% endset %}
        {% do run_query(create_sql) %}

        {% if day_folder is not none %}
            {% set pattern = ".*daily-incremental/" ~ day_folder ~ "/.*" ~ e.file ~ ".*[.]csv" %}
        {% else %}
            {% set pattern = ".*" ~ e.file ~ "[.]csv" %}
        {% endif %}

        {% set select_cols = [] %}
        {% for i in range(1, e.n + 1) %}
            {% do select_cols.append("$" ~ i) %}
        {% endfor %}
        {% do select_cols.append("METADATA$FILENAME") %}
        {% do select_cols.append("METADATA$START_SCAN_TIME") %}

        {% set copy_sql %}
            COPY INTO DBT_HR_ANALYTICS.BRONZE.{{ e.table }}
            FROM (
                SELECT {{ select_cols | join(', ') }}
                FROM @{{ stage }}
            )
            FILE_FORMAT = (FORMAT_NAME = '{{ ff }}')
            PATTERN = '{{ pattern }}'
            ON_ERROR = 'CONTINUE'
        {% endset %}
        {% do run_query(copy_sql) %}
        {{ log("Loaded BRONZE." ~ e.table, info=True) }}

    {% endfor %}

    {{ log("Bronze load complete.", info=True) }}

{% endmacro %}
