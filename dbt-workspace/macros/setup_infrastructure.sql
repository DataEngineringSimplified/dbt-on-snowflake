{#
  Type: RUN-OPERATION MACRO. Does not override any built-in dbt macro.

  One-time bootstrap: creates file format, S3 stage, and governance tags.
  All idempotent (IF NOT EXISTS).

  Params:  None (hardcoded to DBT_HR_ANALYTICS.UTIL and .GOVERNANCE schemas).
  Result:  CSV_FF, MY_S3_STAGE, and 4 tags (ENV, DATA_CLASSIFICATION, PII, DATA_DOMAIN).

  SQL: CREATE FILE FORMAT / CREATE STAGE / CREATE TAG (x4)

  Usage: dbt run-operation setup_infrastructure
#}
{% macro setup_infrastructure() %}

    {% set sql %}

    -- File format
    CREATE FILE FORMAT IF NOT EXISTS DBT_HR_ANALYTICS.UTIL.CSV_FF
        TYPE = 'CSV'
        SKIP_HEADER = 1
        TRIM_SPACE = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('', 'NULL', 'null')
        ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
        COMMENT = 'Standard CSV format: comma-delimited, header row skipped, optional double-quote enclosure.';

    -- External stage (same S3 bucket as legacy)
    CREATE STAGE IF NOT EXISTS DBT_HR_ANALYTICS.UTIL.MY_S3_STAGE
        FILE_FORMAT = DBT_HR_ANALYTICS.UTIL.CSV_FF
        DIRECTORY = (ENABLE = TRUE)
        COMMENT = 'Internal S3 stage for HR analytics source CSV files.';

    -- Governance tags
    CREATE TAG IF NOT EXISTS DBT_HR_ANALYTICS.GOVERNANCE.ENV_TAG
        ALLOWED_VALUES 'DEV', 'QA', 'PROD'
        COMMENT = 'Environment classification tag applied at database/schema level.';

    CREATE TAG IF NOT EXISTS DBT_HR_ANALYTICS.GOVERNANCE.DATA_CLASSIFICATION_TAG
        ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
        COMMENT = 'Data sensitivity classification tag applied at column level.';

    CREATE TAG IF NOT EXISTS DBT_HR_ANALYTICS.GOVERNANCE.PII_TAG
        ALLOWED_VALUES 'NAME', 'EMAIL', 'NONE'
        COMMENT = 'PII sub-classification tag applied at column level.';

    CREATE TAG IF NOT EXISTS DBT_HR_ANALYTICS.GOVERNANCE.DATA_DOMAIN_TAG
        ALLOWED_VALUES 'HR', 'PROJECT', 'ACCESS', 'FINANCE'
        COMMENT = 'Business domain tag applied at table level.';

    {% endset %}

    {% do run_query(sql) %}
    {{ log("Infrastructure setup complete: file format, stage, and governance tags created.", info=True) }}

{% endmacro %}
