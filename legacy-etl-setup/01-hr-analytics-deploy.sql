-- ============================================================================
-- HR_ANALYTICS: Consolidated deployment script (internal stage, no S3)
-- ============================================================================
-- Merged from etl-sp-stream-task-v2 (V001..V010).
-- Changes from original:
--   1. Removed S3 storage integration (HR_ANALYTICS_S3_INT)
--   2. Replaced external S3_CSV_STAGE with internal CSV_STAGE
--   3. All stage references updated from S3_CSV_STAGE -> CSV_STAGE
--
-- Execution order preserves dependency: foundation -> bronze DDL -> streams
-- -> bronze load -> silver DDL -> gold DDL -> silver SPs -> gold SPs
-- -> orchestration -> governance/metrics/semantic view
-- ============================================================================


-- ============================================================================
-- SECTION: V001 - Foundation, Governance Tags, File Format, Internal Stage, Utility Function, Logging
-- ============================================================================

-- V001__foundation.sql
-- Fresh-install foundation. Object creation is idempotent; deployment role grants are intentionally externalized.
-- V001__create_database_schemas.sql
-- Purpose: Create the HR_ANALYTICS database and the medallion schemas
--          (BRONZE, SILVER, GOLD) plus GOVERNANCE for tags/classification
--          and UTIL for shared file formats/stages.
-- Layer:   Foundation
-- ---------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

-- A transient database has no Fail-safe and Snowflake caps all child-object
-- Time Travel at one day. Gold/Governance therefore cannot use seven days
-- unless HR_ANALYTICS is changed to a permanent database.
CREATE TRANSIENT DATABASE IF NOT EXISTS HR_ANALYTICS
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = '{"purpose": "HR Analytics medallion architecture (bronze/silver/gold) built from synthetic HR/IT-consulting data", "owner": "data-platform", "domain": "HR Analytics"}';

-- The remaining unqualified foundation objects are created in this database.
USE DATABASE HR_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = '{"purpose": "Raw, append-only landing zone for source CSV files. No transformations, no updates.", "layer": "bronze"}';

CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = '{"purpose": "Typed, cleansed, append-only tables sourced from BRONZE streams. No updates, no SCD.", "layer": "silver"}';

CREATE SCHEMA IF NOT EXISTS GOLD
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = '{"purpose": "Business-ready star schema (facts/dimensions) with SCD Type-2 history and hash-key relationships.", "layer": "gold"}';

CREATE SCHEMA IF NOT EXISTS GOVERNANCE
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = '{"purpose": "Central governance objects: tags (env/classification/domain), ETL logging, and control tables.", "layer": "governance"}';

CREATE SCHEMA IF NOT EXISTS UTIL
    COMMENT = '{"purpose": "Shared utility objects: file formats and stages used to land source files into BRONZE.", "layer": "util"}';
-- V002__create_governance_tags.sql
-- Purpose: Central governance schema objects - environment tag, data
--          classification tag, and data-domain tag - applied later to
--          SILVER/GOLD columns for auditability and policy enforcement.
-- Layer:   Governance
-- ---------------------------------------------------------------------------

USE SCHEMA GOVERNANCE;

CREATE TAG IF NOT EXISTS ENV_TAG
    ALLOWED_VALUES 'DEV', 'QA', 'PROD'
    COMMENT = 'Environment classification tag applied at database/schema level to identify deployment environment.';

CREATE TAG IF NOT EXISTS DATA_CLASSIFICATION_TAG
    ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED'
    COMMENT = 'Data sensitivity classification tag applied at column level to drive masking/access policies.';

CREATE TAG IF NOT EXISTS PII_TAG
    ALLOWED_VALUES 'NAME', 'EMAIL', 'NONE'
    COMMENT = 'Personally Identifiable Information (PII) sub-classification tag applied at column level.';

CREATE TAG IF NOT EXISTS DATA_DOMAIN_TAG
    ALLOWED_VALUES 'HR', 'PROJECT', 'ACCESS', 'FINANCE'
    COMMENT = 'Business domain tag applied at table level to support data-product grouping and discovery.';

-- Tag the database/schemas with the environment tag (DEV for this build).
ALTER DATABASE HR_ANALYTICS SET TAG HR_ANALYTICS.GOVERNANCE.ENV_TAG = 'DEV';
-- Internal stage for CSV ingestion.
-- Layer:   Util / Bronze ingress
-- ---------------------------------------------------------------------------

USE SCHEMA UTIL;

CREATE FILE FORMAT IF NOT EXISTS CSV_FF
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE
    COMMENT = 'Standard CSV format for synthetic-data source files: comma-delimited, header row skipped, optional double-quote enclosure.';


CREATE STAGE IF NOT EXISTS CSV_STAGE
    FILE_FORMAT = CSV_FF
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal HR Analytics stage for CSV files: full-load/ for base files and daily-incremental/day_NN/ for deltas.';

-- Shared scalar rule: keeping the ordinal mapping in one function prevents
-- employee-skill and project-skill loaders from drifting apart.
CREATE OR REPLACE FUNCTION UTIL.FN_PROFICIENCY_RANK(PROFICIENCY_LEVEL VARCHAR)
RETURNS NUMBER(1,0)
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns the canonical proficiency ordinal: Beginner=1, Intermediate=2, Advanced=3, Expert=4; otherwise null.'
AS
$$
    CASE UPPER(TRIM(PROFICIENCY_LEVEL))
        WHEN 'BEGINNER' THEN 1
        WHEN 'INTERMEDIATE' THEN 2
        WHEN 'ADVANCED' THEN 3
        WHEN 'EXPERT' THEN 4
        ELSE NULL
    END
$$;
-- V008__create_governance_log_table.sql
-- Purpose: Central ETL logging table used by every SILVER and GOLD stored
--          procedure to record execution status, row counts, and errors for
--          auditability and troubleshooting.
-- Layer:   Governance
-- ---------------------------------------------------------------------------

USE SCHEMA GOVERNANCE;

CREATE TABLE IF NOT EXISTS ETL_LOG (
    log_id          NUMBER IDENTITY START 1 INCREMENT 1 COMMENT 'Surrogate key for the log entry.',
    procedure_name  VARCHAR(200)   COMMENT 'Name of the stored procedure that produced this log entry.',
    target_layer    VARCHAR(20)    COMMENT 'Medallion layer written to: BRONZE, SILVER, or GOLD.',
    target_object   VARCHAR(200)   COMMENT 'Fully-qualified target table written to by this run.',
    status          VARCHAR(20)    COMMENT 'Execution outcome: STARTED, SUCCESS, or FAILED.',
    rows_processed  NUMBER(38,0)   COMMENT 'Number of rows inserted/updated/merged during this run.',
    error_message   VARCHAR(4000)  COMMENT 'Exception message captured if the run failed; null on success.',
    start_ts        TIMESTAMP_NTZ  COMMENT 'Timestamp the procedure run started.',
    end_ts          TIMESTAMP_NTZ  COMMENT 'Timestamp the procedure run ended.',
    duration_ms     NUMBER(38,0)   COMMENT 'Run duration in milliseconds.'
)
COMMENT = '{"purpose": "Centralized execution log for all bronze/silver/gold ETL stored procedures.", "grain": "one row per stored procedure execution", "layer": "governance"}';
-- V020__create_quarantine_log_table.sql
-- Purpose: Central quarantine table for rows that fail SILVER data-quality
--          validation (required-field NOT NULL checks, foreign-key existence
--          checks) while loading from BRONZE. Quarantined rows are recorded
--          here with enough context to investigate/replay them, and are
--          excluded from SILVER (never silently dropped, never silently
--          loaded). Mirrors GOVERNANCE.ETL_LOG conventions.
-- Layer:   Governance
-- ---------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS QUARANTINE_LOG (
    quarantine_id     NUMBER IDENTITY START 1 INCREMENT 1 COMMENT 'Surrogate key for the quarantine entry.',
    source_layer      VARCHAR(20)   COMMENT 'Medallion layer the row was rejected while loading into, e.g. SILVER.',
    source_table      VARCHAR(200)  COMMENT 'Fully-qualified BRONZE table the raw row was read from.',
    target_table      VARCHAR(200)  COMMENT 'Fully-qualified SILVER table the row was rejected from (never inserted).',
    natural_key_value VARCHAR(200)  COMMENT 'Source business-key value of the rejected row, for traceability.',
    src_delta_date    DATE          COMMENT 'delta_date of the source row if it came from a daily-delta file; null for base-load rows.',
    src_file_name     VARCHAR(500)  COMMENT 'Source stage file name the row was loaded from (carried through from BRONZE __STG_FILE_NAME).',
    rejection_reason  VARCHAR(500)  COMMENT 'Human-readable reason the row was quarantined, e.g. NULL_REQUIRED_FIELD:skill_id, FK_NOT_FOUND:company_id=999999.',
    raw_row_variant   VARIANT       COMMENT 'Full raw BRONZE row (as JSON) preserved for replay/investigation.',
    quarantined_at    TIMESTAMP_NTZ COMMENT 'Timestamp the row was written to quarantine.'
)
COMMENT = '{"purpose": "Rows that failed NOT-NULL or foreign-key validation while loading BRONZE into SILVER, excluded from SILVER and preserved here for investigation/replay.", "grain": "one row per rejected source record", "layer": "governance"}';
-- V016__create_event_logging.sql
-- Purpose: Native Snowflake event-table logging for the ETL pipeline.
--   LANGUAGE SQL stored procedures have no logger API of their own, so a
--   lightweight LANGUAGE PYTHON helper procedure (GOVERNANCE.SP_LOG_EVENT)
--   is used as the logging entry point: it uses Python's standard `logging`
--   module, which Snowflake automatically routes to the account's event
--   table (SNOWFLAKE.TELEMETRY.EVENTS by default) once LOG_LEVEL is set on
--   the database/schema. Every SQL load procedure (SILVER + GOLD) calls this
--   helper at START / SUCCESS / FAILED checkpoints, in addition to writing
--   to GOVERNANCE.ETL_LOG - ETL_LOG remains the fast, structured, queryable
--   audit trail; the event table is Snowflake's native observability surface
--   (usable with Trail/APM tooling, alerting on log patterns, etc.).
-- Layer:   Governance / observability
-- ---------------------------------------------------------------------------


-- Route INFO-and-above log records emitted anywhere in HR_ANALYTICS to the
-- account's event table (SNOWFLAKE.TELEMETRY.EVENTS by default).
ALTER DATABASE HR_ANALYTICS SET LOG_LEVEL = 'INFO';

-- SP_LOG_EVENT steps: normalise the requested level, emit the message through
-- Snowflake's Python logging integration, then return a compact confirmation.
CREATE OR REPLACE PROCEDURE GOVERNANCE.SP_LOG_EVENT(LOG_LEVEL VARCHAR, MESSAGE VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'log_event'
COMMENT = 'Logging entry point for SQL scripting stored procedures. Emits a structured log record via Python logging, which Snowflake routes to the account event table (SNOWFLAKE.TELEMETRY.EVENTS) once LOG_LEVEL is enabled on the database/schema. LOG_LEVEL argument: INFO, WARN, or ERROR.'
EXECUTE AS OWNER
AS
$$
import logging

logger = logging.getLogger("hr_analytics.etl")

def log_event(session, log_level: str, message: str) -> str:
    level = (log_level or "INFO").upper()
    if level == "ERROR":
        logger.error(message)
    elif level in ("WARN", "WARNING"):
        logger.warning(message)
    else:
        logger.info(message)
    return f"logged [{level}] {message}"
$$
;



-- ============================================================================
-- SECTION: V002 - Bronze Tables (10 raw landing tables)
-- ============================================================================

-- V005__create_bronze_tables.sql
USE ROLE ACCOUNTADMIN;

-- Purpose: Create BRONZE tables for each synthetic-data source CSV.
--          Column names/types are derived from INFER_SCHEMA output (run
--          against @HR_ANALYTICS.UTIL.CSV_STAGE). Every table
--          carries three audit/metadata columns for load traceability:
--            __STG_FILE_NAME        - source file name (METADATA$FILENAME)
--            __STG_FILE_ROW_NUMBER  - row number within source file (METADATA$FILE_ROW_NUMBER)
--            __STG_LOAD_TS          - timestamp the row was loaded into BRONZE
-- Layer:   Bronze (raw, append-only, no updates/deletes)
-- ---------------------------------------------------------------------------

USE DATABASE HR_ANALYTICS;
USE SCHEMA BRONZE;

-- 1. Departments -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DEPARTMENTS (
    department_id   NUMBER(4,0)   COMMENT 'Source business key for the department (raw, untyped-checked in bronze).',
    department_code VARCHAR(20)   COMMENT 'Short department code as provided by source, e.g. WEB, BIG.',
    department_name VARCHAR(200)  COMMENT 'Full department name as provided by source.',
    added_date       VARCHAR(50)  COMMENT 'Raw added_date string from source CSV (typed in SILVER).',
    updated_date      VARCHAR(50) COMMENT 'Raw updated_date string from source CSV (typed in SILVER).',
    is_active        VARCHAR(20) COMMENT 'Raw is_active flag string from source CSV (typed in SILVER).',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 01_departments_master.csv - department master data.", "grain": "one row per department per load", "layer": "bronze", "source_file": "01_departments_master.csv"}';

-- 2. Offices ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS OFFICES (
    office_id      NUMBER(4,0)   COMMENT 'Source business key for the office.',
    office_code    VARCHAR(20)   COMMENT 'Short office code as provided by source.',
    office_city    VARCHAR(200)  COMMENT 'City where the office is located.',
    office_country VARCHAR(200)  COMMENT 'Country where the office is located.',
    office_region  VARCHAR(100)  COMMENT 'Geographic region grouping for the office (e.g. APAC).',
    added_date     VARCHAR(50)   COMMENT 'Raw added_date string from source CSV (typed in SILVER).',
    updated_date    VARCHAR(50)  COMMENT 'Raw updated_date string from source CSV (typed in SILVER).',
    is_active      VARCHAR(20)   COMMENT 'Raw is_active flag string from source CSV (typed in SILVER).',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 02_offices_master.csv - office master data.", "grain": "one row per office per load", "layer": "bronze", "source_file": "02_offices_master.csv"}';

-- 3. Companies ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS COMPANIES (
    company_id             NUMBER(4,0)  COMMENT 'Source business key for the client company.',
    company_name           VARCHAR(300) COMMENT 'Client company name.',
    industry               VARCHAR(200) COMMENT 'Industry vertical of the client company.',
    company_country        VARCHAR(200) COMMENT 'Country of the client company.',
    company_classification VARCHAR(50)  COMMENT 'Client tier classification, e.g. Gold/Silver.',
    added_date              VARCHAR(50) COMMENT 'Raw added_date string from source CSV (typed in SILVER).',
    updated_date             VARCHAR(50) COMMENT 'Raw updated_date string from source CSV (typed in SILVER).',
    is_active               VARCHAR(20) COMMENT 'Raw is_active flag string from source CSV (typed in SILVER).',
    __SRC_DELTA_DATE     VARCHAR(50) COMMENT 'Raw business effective date from a delta file; null for the base load.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Raw delta operation from a delta file; null for the base load.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 03_companies_master.csv - client company master data.", "grain": "one row per client company per load", "layer": "bronze", "source_file": "03_companies_master.csv"}';

-- 4. Employees ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EMPLOYEES (
    employee_id         NUMBER(4,0)  COMMENT 'Source business key for the employee.',
    access_id           VARCHAR(30)  COMMENT 'Badge/access-system identifier linked to employee_daily_access.',
    employee_name       VARCHAR(300) COMMENT 'Employee full name.',
    employee_email      VARCHAR(300) COMMENT 'Employee corporate email address.',
    department_id       NUMBER(4,0)  COMMENT 'FK (raw) to departments.department_id.',
    office_id           NUMBER(4,0)  COMMENT 'FK (raw) to offices.office_id.',
    manager_employee_id NUMBER(4,0)  COMMENT 'FK (raw) to employees.employee_id for the reporting manager; null for top-level roles.',
    job_title           VARCHAR(300) COMMENT 'Employee job title.',
    job_level           VARCHAR(50)  COMMENT 'Employee job level/band, e.g. Director, Manager.',
    employment_status   VARCHAR(50)  COMMENT 'Current employment status, e.g. Active, Terminated.',
    hire_date            VARCHAR(50) COMMENT 'Raw hire_date string from source CSV (typed in SILVER).',
    added_date           VARCHAR(50) COMMENT 'Raw added_date string from source CSV (typed in SILVER).',
    updated_date          VARCHAR(50) COMMENT 'Raw updated_date string from source CSV (typed in SILVER).',
    is_active            VARCHAR(20) COMMENT 'Raw is_active flag string from source CSV (typed in SILVER).',
    __SRC_DELTA_DATE     VARCHAR(50) COMMENT 'Raw business effective date from a delta file; null for the base load.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Raw delta operation from a delta file; null for the base load.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 04_employees_master.csv - employee master/org-chart data.", "grain": "one row per employee per load", "layer": "bronze", "source_file": "04_employees_master.csv"}';

-- 5. Projects ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PROJECTS (
    project_id             NUMBER(5,0)   COMMENT 'Source business key for the project.',
    project_name           VARCHAR(400)  COMMENT 'Project name.',
    company_id             NUMBER(4,0)   COMMENT 'FK (raw) to companies.company_id - the client the project is delivered for.',
    owning_department_id   NUMBER(4,0)   COMMENT 'FK (raw) to departments.department_id owning the project.',
    project_type           VARCHAR(100)  COMMENT 'Type of engagement, e.g. Modernization, Managed Services.',
    project_billing_type   VARCHAR(50)   COMMENT 'Billing arrangement, e.g. Fixed Bid, Time & Materials.',
    project_budget_usd     NUMBER(12,2)  COMMENT 'Approved project budget in USD.',
    project_status         VARCHAR(50)   COMMENT 'Current project status, e.g. Active, Completed.',
    start_date              VARCHAR(50)  COMMENT 'Raw start_date string from source CSV (typed in SILVER).',
    planned_end_date        VARCHAR(50)  COMMENT 'Raw planned_end_date string from source CSV (typed in SILVER).',
    actual_end_date         VARCHAR(50)  COMMENT 'Raw actual_end_date string from source CSV (typed in SILVER); null while project is open.',
    added_date              VARCHAR(50)  COMMENT 'Raw added_date string from source CSV (typed in SILVER).',
    updated_date             VARCHAR(50) COMMENT 'Raw updated_date string from source CSV (typed in SILVER).',
    is_active               VARCHAR(20)  COMMENT 'Raw is_active flag string from source CSV (typed in SILVER).',
    __SRC_DELTA_DATE     VARCHAR(50) COMMENT 'Raw business effective date from a delta file; null for the base load.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Raw delta operation from a delta file; null for the base load.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 05_projects_master.csv - project master data.", "grain": "one row per project per load", "layer": "bronze", "source_file": "05_projects_master.csv"}';

-- 6. Employee <-> Project assignments --------------------------------------------
CREATE TABLE IF NOT EXISTS EMPLOYEE_PROJECT_ASSIGNMENTS (
    assignment_id           NUMBER(5,0)  COMMENT 'Source business key for the assignment record.',
    employee_id             NUMBER(4,0)  COMMENT 'FK (raw) to employees.employee_id.',
    project_id              NUMBER(5,0)  COMMENT 'FK (raw) to projects.project_id.',
    assignment_role         VARCHAR(200) COMMENT 'Role the employee performs on the project.',
    allocation_percent      NUMBER(3,0)  COMMENT 'Percentage of employee time allocated to the project.',
    assignment_start_date    VARCHAR(50) COMMENT 'Raw assignment_start_date string from source CSV (typed in SILVER).',
    assignment_end_date      VARCHAR(50) COMMENT 'Raw assignment_end_date string from source CSV (typed in SILVER); null while assignment is active.',
    __SRC_DELTA_DATE     VARCHAR(50) COMMENT 'Raw business effective date from a delta file; null for the base load.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Raw delta operation from a delta file; null for the base load.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 06_employee_project_assignments.csv - employee-to-project staffing events.", "grain": "one row per employee/project assignment per load", "layer": "bronze", "source_file": "06_employee_project_assignments.csv"}';

-- 7. Employee daily access (badge events) ----------------------------------------
CREATE TABLE IF NOT EXISTS EMPLOYEE_DAILY_ACCESS (
    access_event_id     NUMBER(7,0)   COMMENT 'Source business key for the badge access event.',
    access_id           VARCHAR(30)   COMMENT 'FK (raw) to employees.access_id.',
    office_id           NUMBER(4,0)   COMMENT 'FK (raw) to offices.office_id where the badge event occurred.',
    access_date          VARCHAR(50)  COMMENT 'Raw access_date string from source CSV (typed in SILVER).',
    access_timestamp     VARCHAR(50)  COMMENT 'Raw access_timestamp string from source CSV (typed in SILVER).',
    access_event_type   VARCHAR(20)   COMMENT 'Badge event direction, e.g. IN / OUT.',
    office_city         VARCHAR(200)  COMMENT 'Denormalized office city at time of event (from source system).',
    __SRC_DELTA_DATE     VARCHAR(50) COMMENT 'Raw business effective date from a delta file; null for the base load.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Raw delta operation from a delta file; null for the base load.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 07_employee_daily_access.csv - high-volume badge in/out events.", "grain": "one row per badge access event per load", "layer": "bronze", "source_file": "07_employee_daily_access.csv"}';
CREATE TABLE IF NOT EXISTS SKILLS (
    skill_id       NUMBER(5,0)  COMMENT 'Source business key for the controlled skill.',
    skill_name     VARCHAR(200) COMMENT 'Skill display name, e.g. dbt, Snowflake.',
    skill_category VARCHAR(100) COMMENT 'Skill grouping, e.g. Data, Web Development.',
    added_date      VARCHAR(50) COMMENT 'Raw added_date string from source CSV (typed in SILVER).',
    updated_date     VARCHAR(50) COMMENT 'Raw updated_date string from source CSV (typed in SILVER).',
    is_active       VARCHAR(20) COMMENT 'Raw is_active flag string from source CSV (typed in SILVER).',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 08_skills_master.csv - controlled skill/technology master data.", "grain": "one row per skill per load", "layer": "bronze", "source_file": "08_skills_master.csv"}';

-- 2b. Employee <-> skill (proficiency) bridge - has delta files.
CREATE TABLE IF NOT EXISTS EMPLOYEE_SKILLS (
    employee_skill_id  NUMBER(6,0)  COMMENT 'Source business key for the employee-skill relationship record.',
    employee_id         NUMBER(4,0) COMMENT 'FK (raw) to employees.employee_id.',
    skill_id             NUMBER(5,0) COMMENT 'FK (raw) to skills.skill_id.',
    proficiency_level   VARCHAR(50)  COMMENT 'Self/assessed proficiency band, e.g. Beginner..Expert.',
    is_primary_skill    VARCHAR(20)  COMMENT 'Raw is_primary_skill flag string from source CSV (typed in SILVER).',
    __SRC_DELTA_DATE     VARCHAR(50) COMMENT 'Raw delta_date string from a daily-delta file; NULL for one-time base-load rows (typed in SILVER).',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Raw operation_type string from a daily-delta file (e.g. UPDATE); NULL for one-time base-load rows.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 09_employee_skills.csv and its daily-delta files - employee-to-skill proficiency bridge.", "grain": "one row per employee/skill relationship version per load", "layer": "bronze", "source_file": "09_employee_skills.csv"}';

-- 2c. Project <-> required-technology bridge - has delta files.
CREATE TABLE IF NOT EXISTS PROJECT_TECHNOLOGIES (
    project_technology_id      NUMBER(6,0)  COMMENT 'Source business key for the project-technology requirement record.',
    project_id                  NUMBER(5,0) COMMENT 'FK (raw) to projects.project_id.',
    skill_id                     NUMBER(5,0) COMMENT 'FK (raw) to skills.skill_id.',
    required_proficiency_level VARCHAR(50)  COMMENT 'Minimum proficiency band required, e.g. Beginner..Expert.',
    is_primary_technology      VARCHAR(20)  COMMENT 'Raw is_primary_technology flag string from source CSV (typed in SILVER).',
    __SRC_DELTA_DATE            VARCHAR(50) COMMENT 'Raw delta_date string from a daily-delta file; NULL for one-time base-load rows (typed in SILVER).',
    __SRC_OPERATION_TYPE        VARCHAR(20) COMMENT 'Raw operation_type string from a daily-delta file (e.g. UPDATE); NULL for one-time base-load rows.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Source stage file name the row was loaded from (METADATA$FILENAME).',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Row number of the record within its source file (METADATA$FILE_ROW_NUMBER).',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Timestamp the row was loaded into the BRONZE layer.'
)
COMMENT = '{"purpose": "Raw landing of 10_project_technologies.csv and its daily-delta files - project-to-required-technology bridge.", "grain": "one row per project/skill requirement version per load", "layer": "bronze", "source_file": "10_project_technologies.csv"}';



-- ============================================================================
-- SECTION: V003 - Bronze Streams (10 append-only CDC streams)
-- ============================================================================

-- V003__bronze_streams.sql
-- Streams are created after all Bronze tables so every landing table has one append-only Silver feed.
USE ROLE ACCOUNTADMIN;
USE DATABASE HR_ANALYTICS;
USE SCHEMA BRONZE;

-- V006__create_bronze_streams.sql
-- Purpose: Append-only streams on every BRONZE table. BRONZE is insert-only
--          (no updates/deletes), so APPEND_ONLY streams give SILVER load
--          procedures an efficient, offset-tracked view of newly landed rows
--          only - this is what prevents SILVER from re-loading/duplicating
--          data that has already been processed.
-- Layer:   Bronze -> Silver change-data-capture
-- ---------------------------------------------------------------------------


CREATE STREAM IF NOT EXISTS DEPARTMENTS_STRM
    ON TABLE DEPARTMENTS
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.DEPARTMENTS with newly landed BRONZE rows only.';

CREATE STREAM IF NOT EXISTS OFFICES_STRM
    ON TABLE OFFICES
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.OFFICES with newly landed BRONZE rows only.';

CREATE STREAM IF NOT EXISTS COMPANIES_STRM
    ON TABLE COMPANIES
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.COMPANIES with newly landed BRONZE rows only.';

CREATE STREAM IF NOT EXISTS EMPLOYEES_STRM
    ON TABLE EMPLOYEES
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.EMPLOYEES with newly landed BRONZE rows only.';

CREATE STREAM IF NOT EXISTS PROJECTS_STRM
    ON TABLE PROJECTS
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.PROJECTS with newly landed BRONZE rows only.';

CREATE STREAM IF NOT EXISTS EMPLOYEE_PROJECT_ASSIGNMENTS_STRM
    ON TABLE EMPLOYEE_PROJECT_ASSIGNMENTS
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS with newly landed BRONZE rows only.';

CREATE STREAM IF NOT EXISTS EMPLOYEE_DAILY_ACCESS_STRM
    ON TABLE EMPLOYEE_DAILY_ACCESS
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.EMPLOYEE_DAILY_ACCESS with newly landed BRONZE rows only.';
CREATE STREAM IF NOT EXISTS SKILLS_STRM
    ON TABLE SKILLS
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.SKILLS with newly landed BRONZE rows only.';

CREATE STREAM IF NOT EXISTS EMPLOYEE_SKILLS_STRM
    ON TABLE EMPLOYEE_SKILLS
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.EMPLOYEE_SKILLS with newly landed BRONZE rows only.';

CREATE STREAM IF NOT EXISTS PROJECT_TECHNOLOGIES_STRM
    ON TABLE PROJECT_TECHNOLOGIES
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only CDC stream feeding SILVER.PROJECT_TECHNOLOGIES with newly landed BRONZE rows only.';



-- ============================================================================
-- SECTION: V004 - Load Bronze from Internal Stage (base load)
-- ============================================================================

-- V004__load_bronze_from_internal_stage.sql
-- Base-load ingestion happens only after V003 creates every append-only
-- Bronze stream. Each COPY is therefore visible to its corresponding stream
-- and will be consumed exactly once by the Silver loaders.
-- File names may be plain CSV or gzip-compressed CSV; Snowflake detects the
-- compression automatically from the file extension.

USE ROLE ACCOUNTADMIN;
USE DATABASE HR_ANALYTICS;
USE SCHEMA BRONZE;

-- COPY steps: select source fields plus immutable stage metadata from the
-- external full-load prefix, insert into Bronze, and leave delta lineage null
-- because these are the bootstrap records rather than daily changes.

COPY INTO DEPARTMENTS (department_id, department_code, department_name, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*01_departments_master\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO OFFICES (office_id, office_code, office_city, office_country, office_region, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*02_offices_master\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO COMPANIES (company_id, company_name, industry, company_country, company_classification, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*03_companies_master\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO EMPLOYEES (employee_id, access_id, employee_name, employee_email, department_id, office_id, manager_employee_id, job_title, job_level, employment_status, hire_date, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*04_employees_master\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO PROJECTS (project_id, project_name, company_id, owning_department_id, project_type, project_billing_type, project_budget_usd, project_status, start_date, planned_end_date, actual_end_date, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*05_projects_master\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO EMPLOYEE_PROJECT_ASSIGNMENTS (assignment_id, employee_id, project_id, assignment_role, allocation_percent, assignment_start_date, assignment_end_date, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*06_employee_project_assignments\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO EMPLOYEE_DAILY_ACCESS (access_event_id, access_id, office_id, access_date, access_timestamp, access_event_type, office_city, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*07_employee_daily_access\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO SKILLS (skill_id, skill_name, skill_category, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*08_skills_master\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO EMPLOYEE_SKILLS (employee_skill_id, employee_id, skill_id, proficiency_level, is_primary_skill, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*09_employee_skills\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO PROJECT_TECHNOLOGIES (project_technology_id, project_id, skill_id, required_proficiency_level, is_primary_technology, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*10_project_technologies\\.csv(\\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');



-- ============================================================================
-- SECTION: V005 - Silver Tables (10 typed/cleansed tables)
-- ============================================================================

-- V005__silver_tables.sql
USE ROLE ACCOUNTADMIN;

-- Purpose: Typed, cleansed SILVER tables - one per BRONZE source. SILVER is
--          insert-only (no updates, no SCD); each row is a straight,
--          properly-typed copy of a BRONZE row that has not yet been
--          processed (tracked via the BRONZE stream offset consumed by the
--          corresponding load procedure). Technical/audit columns carry a
--          "__" prefix to visually distinguish them from business columns.
--
--          Standardization/derived-attribute columns introduced by
--          V035__silver_enrichment_transformations.sql (country/currency,
--          email/identity normalization, org hierarchy validation, project
--          lifecycle conformance, assignment capacity normalization, access
--          conformance) are embedded directly here rather than added later
--          via ALTER TABLE, so a fresh install gets the final schema in one
--          pass. Country/currency values are derived by an inline CASE
--          statement in the load procedures (no reference table).
-- Layer:   Silver
-- ---------------------------------------------------------------------------

USE DATABASE HR_ANALYTICS;
USE SCHEMA SILVER;

CREATE TABLE IF NOT EXISTS DEPARTMENTS (
    department_id   NUMBER(4,0)   NOT NULL COMMENT 'Business key for the department.',
    department_code VARCHAR(20)   COMMENT 'Short department code, e.g. WEB, BIG.',
    department_name VARCHAR(200)  COMMENT 'Full department name.',
    added_date       DATE         COMMENT 'Date the department record was first added in the source system.',
    updated_date      DATE        COMMENT 'Date the department record was last updated in the source system.',
    is_active        BOOLEAN      COMMENT 'Whether the department is currently active.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed department master data.", "grain": "one row per department", "layer": "silver", "load_pattern": "insert-only from DEPARTMENTS stream"}';

CREATE TABLE IF NOT EXISTS OFFICES (
    office_id      NUMBER(4,0)   NOT NULL COMMENT 'Business key for the office.',
    office_code    VARCHAR(20)   COMMENT 'Short office code.',
    office_city    VARCHAR(200)  COMMENT 'City where the office is located.',
    office_country VARCHAR(200)  COMMENT 'Country where the office is located.',
    office_region  VARCHAR(100)  COMMENT 'Geographic region grouping for the office (e.g. APAC).',
    added_date     DATE          COMMENT 'Date the office record was first added in the source system.',
    updated_date    DATE         COMMENT 'Date the office record was last updated in the source system.',
    is_active      BOOLEAN       COMMENT 'Whether the office is currently active.',
    country_iso2               VARCHAR(2)   COMMENT 'ISO-3166 alpha-2 country code, derived from office_country via inline CASE in SP_LOAD_SILVER_OFFICES (no reference table).',
    country_iso3               VARCHAR(3)   COMMENT 'ISO-3166 alpha-3 country code, derived the same way as country_iso2.',
    country_name_standardized  VARCHAR(200) COMMENT 'Standardized country display name, derived the same way as country_iso2.',
    default_currency_code      VARCHAR(3)   COMMENT 'ISO-4217 currency code for the office country, derived the same way as country_iso2.',
    is_region_valid             BOOLEAN     COMMENT 'True when office_region is one of the observed APAC/EU/NA values; informational flag only (tmp-transformation.md), not a quarantine gate.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed office master data.", "grain": "one row per office", "layer": "silver", "load_pattern": "insert-only from OFFICES stream"}';

CREATE TABLE IF NOT EXISTS COMPANIES (
    company_id             NUMBER(4,0)  NOT NULL COMMENT 'Business key for the client company.',
    company_name           VARCHAR(300) COMMENT 'Client company name.',
    industry               VARCHAR(200) COMMENT 'Industry vertical of the client company.',
    company_country        VARCHAR(200) COMMENT 'Country of the client company.',
    company_classification VARCHAR(50)  COMMENT 'Client tier classification, e.g. Gold/Silver.',
    added_date              DATE        COMMENT 'Date the company record was first added in the source system.',
    updated_date             DATE       COMMENT 'Date the company record was last updated in the source system.',
    is_active               BOOLEAN     COMMENT 'Whether the client company is currently active.',
    country_iso2               VARCHAR(2)   COMMENT 'ISO-3166 alpha-2 country code, derived from company_country via inline CASE in SP_LOAD_SILVER_COMPANIES (no reference table).',
    country_iso3               VARCHAR(3)   COMMENT 'ISO-3166 alpha-3 country code, derived the same way as country_iso2.',
    country_name_standardized  VARCHAR(200) COMMENT 'Standardized country display name, derived the same way as country_iso2.',
    default_currency_code      VARCHAR(3)   COMMENT 'ISO-4217 currency code for the company country, derived the same way as country_iso2.',
    __SRC_DELTA_DATE     DATE        COMMENT 'Technical column: business effective date supplied by a delta file; null for base rows.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Technical column: delta operation supplied by source; null for base rows.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed client company master data.", "grain": "one row per client company", "layer": "silver", "load_pattern": "insert-only from COMPANIES stream"}';

CREATE TABLE IF NOT EXISTS EMPLOYEES (
    employee_id         NUMBER(4,0)  NOT NULL COMMENT 'Business key for the employee.',
    access_id           VARCHAR(30)  COMMENT 'Badge/access-system identifier linked to employee_daily_access.',
    employee_name       VARCHAR(300) COMMENT 'Employee full name.',
    employee_email      VARCHAR(300) COMMENT 'Employee corporate email address.',
    department_id       NUMBER(4,0)  COMMENT 'FK to departments.department_id.',
    office_id           NUMBER(4,0)  COMMENT 'FK to offices.office_id.',
    manager_employee_id NUMBER(4,0)  COMMENT 'FK to employees.employee_id for the reporting manager; null for top-level roles.',
    job_title           VARCHAR(300) COMMENT 'Employee job title.',
    job_level           VARCHAR(50)  COMMENT 'Employee job level/band, e.g. Director, Manager.',
    employment_status   VARCHAR(50)  COMMENT 'Current employment status, e.g. Active, Terminated.',
    hire_date            DATE        COMMENT 'Date the employee was hired.',
    added_date           DATE        COMMENT 'Date the employee record was first added in the source system.',
    updated_date          DATE       COMMENT 'Date the employee record was last updated in the source system.',
    is_active            BOOLEAN     COMMENT 'Whether the employee is currently active.',
    email_domain                 VARCHAR(200) COMMENT 'Lowercased domain portion of employee_email, derived in SP_LOAD_SILVER_EMPLOYEES.',
    is_email_domain_valid        BOOLEAN      COMMENT 'True when email_domain = des-dbt.com; null when employee_email is null.',
    employee_name_normalized     VARCHAR(300) COMMENT 'Whitespace-normalized, title-cased employee_name.',
    is_manager_reference_valid   BOOLEAN      COMMENT 'False for a self-reference or an unresolvable manager_employee_id; null when manager_employee_id is null (org root).',
    is_self_manager              BOOLEAN      COMMENT 'True when manager_employee_id = employee_id (should be quarantined, not seen in normal data).',
    is_org_root                  BOOLEAN      COMMENT 'True when manager_employee_id is null (top of the org chart).',
    is_department_manager_match  BOOLEAN      COMMENT 'True when the employee''s department_id matches their manager''s department_id; null when there is no manager.',
    __SRC_DELTA_DATE     DATE        COMMENT 'Technical column: business effective date supplied by a delta file; null for base rows.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Technical column: delta operation supplied by source; null for base rows.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed employee master/org-chart data.", "grain": "one row per employee", "layer": "silver", "load_pattern": "insert-only from EMPLOYEES stream"}';

CREATE TABLE IF NOT EXISTS PROJECTS (
    project_id             NUMBER(5,0)   NOT NULL COMMENT 'Business key for the project.',
    project_name           VARCHAR(400)  COMMENT 'Project name.',
    company_id             NUMBER(4,0)   COMMENT 'FK to companies.company_id - the client the project is delivered for.',
    owning_department_id   NUMBER(4,0)   COMMENT 'FK to departments.department_id owning the project.',
    project_type           VARCHAR(100)  COMMENT 'Type of engagement, e.g. Modernization, Managed Services.',
    project_billing_type   VARCHAR(50)   COMMENT 'Billing arrangement, e.g. Fixed Bid, Time & Materials.',
    project_budget_usd     NUMBER(12,2)  COMMENT 'Approved project budget in USD.',
    project_status         VARCHAR(50)   COMMENT 'Current project status, e.g. Active, Completed.',
    start_date              DATE         COMMENT 'Project start date.',
    planned_end_date        DATE         COMMENT 'Planned project end date.',
    actual_end_date         DATE         COMMENT 'Actual project end date; null while project is open.',
    added_date              DATE         COMMENT 'Date the project record was first added in the source system.',
    updated_date             DATE        COMMENT 'Date the project record was last updated in the source system.',
    is_active               BOOLEAN      COMMENT 'Whether the project is currently active.',
    project_lifecycle_state VARCHAR(20)  COMMENT 'Derived from project_status + dates: PLANNED, IN_FLIGHT, COMPLETED, ON_HOLD, or UNKNOWN.',
    is_date_sequence_valid  BOOLEAN      COMMENT 'False when start_date > planned_end_date, actual_end_date < start_date, or status=Completed with no actual_end_date.',
    __SRC_DELTA_DATE     DATE        COMMENT 'Technical column: business effective date supplied by a delta file; null for base rows.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Technical column: delta operation supplied by source; null for base rows.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed project master data.", "grain": "one row per project", "layer": "silver", "load_pattern": "insert-only from PROJECTS stream"}';

CREATE TABLE IF NOT EXISTS EMPLOYEE_PROJECT_ASSIGNMENTS (
    assignment_id           NUMBER(5,0)  NOT NULL COMMENT 'Business key for the assignment record.',
    employee_id             NUMBER(4,0)  COMMENT 'FK to employees.employee_id.',
    project_id              NUMBER(5,0)  COMMENT 'FK to projects.project_id.',
    assignment_role         VARCHAR(200) COMMENT 'Role the employee performs on the project.',
    allocation_percent      NUMBER(3,0)  COMMENT 'Percentage of employee time allocated to the project.',
    assignment_start_date    DATE        COMMENT 'Date the assignment started.',
    assignment_end_date      DATE        COMMENT 'Date the assignment ended; null while assignment is active.',
    allocation_fraction      NUMBER(6,4) COMMENT 'allocation_percent / 100, for use in fractional-FTE math.',
    is_active_assignment     BOOLEAN     COMMENT 'True when assignment_end_date is null or in the future.',
    is_allocation_valid      BOOLEAN     COMMENT 'False when allocation_percent is outside 0-100; null when allocation_percent is null.',
    __SRC_DELTA_DATE     DATE        COMMENT 'Technical column: business effective date supplied by a delta file; null for base rows.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Technical column: delta operation supplied by source; null for base rows.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed employee-to-project staffing events.", "grain": "one row per employee/project assignment", "layer": "silver", "load_pattern": "insert-only from EMPLOYEE_PROJECT_ASSIGNMENTS stream"}';

CREATE TABLE IF NOT EXISTS EMPLOYEE_DAILY_ACCESS (
    access_event_id     NUMBER(7,0)   NOT NULL COMMENT 'Business key for the badge access event.',
    access_id           VARCHAR(30)   COMMENT 'FK to employees.access_id.',
    office_id           NUMBER(4,0)   COMMENT 'FK to offices.office_id where the badge event occurred.',
    access_date          DATE         COMMENT 'Calendar date of the badge event.',
    access_timestamp     TIMESTAMP_NTZ COMMENT 'Exact timestamp of the badge event.',
    access_event_type   VARCHAR(20)   COMMENT 'Badge event direction, e.g. IN / OUT.',
    office_city         VARCHAR(200)  COMMENT 'Denormalized office city at time of event (from source system).',
    access_date_standardized       DATE         COMMENT 'TRY_TO_DATE(access_date); same value, standardized derivation name per tmp-transformation.md.',
    access_time                     TIME        COMMENT 'Time-of-day portion of access_timestamp.',
    access_hour                     NUMBER(2,0) COMMENT 'Hour-of-day (0-23) portion of access_timestamp.',
    access_event_type_standardized VARCHAR(20)  COMMENT 'UPPER(TRIM(access_event_type)).',
    is_date_consistent              BOOLEAN     COMMENT 'False when DATE(access_timestamp) != access_date; null when either is unparsable.',
    __SRC_DELTA_DATE     DATE        COMMENT 'Technical column: business effective date supplied by a delta file; null for base rows.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Technical column: delta operation supplied by source; null for base rows.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed badge in/out events.", "grain": "one row per badge access event", "layer": "silver", "load_pattern": "insert-only from EMPLOYEE_DAILY_ACCESS stream"}';
CREATE TABLE IF NOT EXISTS SKILLS (
    skill_id       NUMBER(5,0)  NOT NULL COMMENT 'Business key for the controlled skill.',
    skill_name     VARCHAR(200) COMMENT 'Skill display name, e.g. dbt, Snowflake.',
    skill_category VARCHAR(100) COMMENT 'Skill grouping, e.g. Data, Web Development.',
    added_date      DATE        COMMENT 'Date the skill record was first added in the source system.',
    updated_date     DATE        COMMENT 'Date the skill record was last updated in the source system.',
    is_active       BOOLEAN     COMMENT 'Whether the skill is currently active.',
    skill_code      VARCHAR(50) COMMENT 'Canonical short code derived from skill_name via inline CASE in SP_LOAD_SILVER_SKILLS, e.g. SNOWFLAKE, DBT, REACT.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed controlled skill/technology master data.", "grain": "one row per skill", "layer": "silver", "load_pattern": "insert-only from SKILLS stream"}';

CREATE TABLE IF NOT EXISTS EMPLOYEE_SKILLS (
    employee_skill_id  NUMBER(6,0) NOT NULL COMMENT 'Business key for the employee-skill relationship record.',
    employee_id         NUMBER(4,0) COMMENT 'FK to employees.employee_id.',
    skill_id             NUMBER(5,0) COMMENT 'FK to skills.skill_id.',
    proficiency_level   VARCHAR(50) COMMENT 'Self/assessed proficiency band, e.g. Beginner..Expert.',
    is_primary_skill    BOOLEAN     COMMENT 'Whether this is the employee''s primary skill.',
    proficiency_rank    NUMBER(1,0) COMMENT 'Ordinal mapping of proficiency_level: Beginner=1, Intermediate=2, Advanced=3, Expert=4.',
    __SRC_DELTA_DATE     DATE        COMMENT 'Technical column: delta_date of the source row if it came from a daily-delta file; null for base-load rows.',
    __SRC_OPERATION_TYPE VARCHAR(20) COMMENT 'Technical column: operation_type of the source row (e.g. UPDATE) if it came from a daily-delta file; null for base-load rows.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed employee-to-skill proficiency bridge (base load + daily deltas).", "grain": "one row per employee/skill relationship version", "layer": "silver", "load_pattern": "insert-only from EMPLOYEE_SKILLS stream, quarantine on NULL/FK failure"}';

CREATE TABLE IF NOT EXISTS PROJECT_TECHNOLOGIES (
    project_technology_id      NUMBER(6,0) NOT NULL COMMENT 'Business key for the project-technology requirement record.',
    project_id                  NUMBER(5,0) COMMENT 'FK to projects.project_id.',
    skill_id                     NUMBER(5,0) COMMENT 'FK to skills.skill_id.',
    required_proficiency_level VARCHAR(50)  COMMENT 'Minimum proficiency band required, e.g. Beginner..Expert.',
    is_primary_technology      BOOLEAN      COMMENT 'Whether this is a primary technology requirement for the project.',
    required_proficiency_rank  NUMBER(1,0)  COMMENT 'Ordinal mapping of required_proficiency_level: Beginner=1, Intermediate=2, Advanced=3, Expert=4.',
    __SRC_DELTA_DATE            DATE        COMMENT 'Technical column: delta_date of the source row if it came from a daily-delta file; null for base-load rows.',
    __SRC_OPERATION_TYPE        VARCHAR(20) COMMENT 'Technical column: operation_type of the source row (e.g. UPDATE) if it came from a daily-delta file; null for base-load rows.',
    __STG_FILE_NAME          VARCHAR(500)   COMMENT 'Technical column: source stage file name carried through from BRONZE.',
    __STG_FILE_ROW_NUMBER     NUMBER(38,0)  COMMENT 'Technical column: row number within the source file carried through from BRONZE.',
    __STG_LOAD_TS             TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into BRONZE.',
    __SILVER_LOAD_TS          TIMESTAMP_NTZ COMMENT 'Technical column: timestamp the row was loaded into SILVER.'
)
COMMENT = '{"purpose": "Typed, cleansed project-to-required-technology bridge (base load + daily deltas).", "grain": "one row per project/skill requirement version", "layer": "silver", "load_pattern": "insert-only from PROJECT_TECHNOLOGIES stream, quarantine on NULL/FK failure"}';



-- ============================================================================
-- SECTION: V006 - Gold Tables (dimensions, facts, bridges)
-- ============================================================================

-- V006__gold_tables.sql
USE ROLE ACCOUNTADMIN;

-- Purpose: GOLD dimension tables implementing Slowly Changing Dimension
--          Type-2 (SCD2) history. Each dimension has a SHA2-256 surrogate
--          hash key (<entity>_HK) derived from its business key, used as
--          the relationship key for downstream facts and for
--          self/cross-dimension foreign keys. SCD2 bookkeeping columns (date based):
--            __EFFECTIVE_FROM_DATE - first business date this version is active
--            __EFFECTIVE_TO_DATE   - last business date this version is active
--            __IS_CURRENT         - TRUE for the single currently active version per business key
--            __ROW_HASH           - SHA2 hash of tracked attributes, used to detect changes
--            __GOLD_LOAD_TS       - timestamp this version row was written to GOLD
-- Layer:   Gold (dimensions, SCD Type-2)
-- Note:    Primary/foreign keys are declared for lineage/BI-tool consumption;
--          Snowflake does not enforce them at write time.
-- ---------------------------------------------------------------------------

USE DATABASE HR_ANALYTICS;
USE SCHEMA GOLD;

-- 1. DIM_DEPARTMENT ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DIM_DEPARTMENT (
    department_hk    VARCHAR(64)   NOT NULL COMMENT 'Surrogate hash key: SHA2-256(department_id). Primary key of this dimension.',
    department_id    NUMBER(4,0)   NOT NULL COMMENT 'Business key for the department (natural key from source).',
    department_code  VARCHAR(20)   COMMENT 'Short department code, e.g. WEB, BIG.',
    department_name  VARCHAR(200)  COMMENT 'Full department name.',
    is_active         BOOLEAN      COMMENT 'Whether the department is currently active (as of this version).',
    __EFFECTIVE_FROM_DATE DATE COMMENT 'Technical column: first business date this dimension version is effective.',
    __EFFECTIVE_TO_DATE   DATE COMMENT 'Technical column: last business date this dimension version is effective; 9999-12-31 while current.',
    __IS_CURRENT          BOOLEAN      COMMENT 'Technical column: TRUE if this is the current version of the business key.',
    __ROW_HASH            VARCHAR(64)  COMMENT 'Technical column: SHA2 hash of tracked attributes, used to detect changes for SCD2.',
    __GOLD_LOAD_TS         TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this version row was written to GOLD.',
    CONSTRAINT PK_DIM_DEPARTMENT PRIMARY KEY (department_hk)
)
COMMENT = '{"purpose": "Department dimension with SCD Type-2 history.", "grain": "one row per department per changed version", "layer": "gold", "scd_type": 2}';

-- 2. DIM_OFFICE ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DIM_OFFICE (
    office_hk       VARCHAR(64)  NOT NULL COMMENT 'Surrogate hash key: SHA2-256(office_id). Primary key of this dimension.',
    office_id       NUMBER(4,0)  NOT NULL COMMENT 'Business key for the office (natural key from source).',
    office_code     VARCHAR(20)  COMMENT 'Short office code.',
    office_city     VARCHAR(200) COMMENT 'City where the office is located.',
    office_country  VARCHAR(200) COMMENT 'Country where the office is located.',
    office_region   VARCHAR(100) COMMENT 'Geographic region grouping for the office (e.g. APAC).',
    is_active       BOOLEAN      COMMENT 'Whether the office is currently active (as of this version).',
    __EFFECTIVE_FROM_DATE DATE COMMENT 'Technical column: timestamp this dimension version became effective.',
    __EFFECTIVE_TO_DATE   DATE COMMENT 'Technical column: timestamp this dimension version was superseded; 9999-12-31 while current.',
    __IS_CURRENT          BOOLEAN      COMMENT 'Technical column: TRUE if this is the current version of the business key.',
    __ROW_HASH            VARCHAR(64)  COMMENT 'Technical column: SHA2 hash of tracked attributes, used to detect changes for SCD2.',
    __GOLD_LOAD_TS         TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this version row was written to GOLD.',
    CONSTRAINT PK_DIM_OFFICE PRIMARY KEY (office_hk)
)
COMMENT = '{"purpose": "Office dimension with SCD Type-2 history.", "grain": "one row per office per changed version", "layer": "gold", "scd_type": 2}';

-- 3. DIM_COMPANY ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DIM_COMPANY (
    company_hk             VARCHAR(64)  NOT NULL COMMENT 'Surrogate hash key: SHA2-256(company_id). Primary key of this dimension.',
    company_id             NUMBER(4,0)  NOT NULL COMMENT 'Business key for the client company (natural key from source).',
    company_name           VARCHAR(300) COMMENT 'Client company name.',
    industry               VARCHAR(200) COMMENT 'Industry vertical of the client company.',
    company_country        VARCHAR(200) COMMENT 'Country of the client company.',
    company_classification VARCHAR(50)  COMMENT 'Client tier classification, e.g. Gold/Silver.',
    is_active               BOOLEAN     COMMENT 'Whether the client company is currently active (as of this version).',
    __EFFECTIVE_FROM_DATE DATE COMMENT 'Technical column: timestamp this dimension version became effective.',
    __EFFECTIVE_TO_DATE   DATE COMMENT 'Technical column: timestamp this dimension version was superseded; 9999-12-31 while current.',
    __IS_CURRENT          BOOLEAN      COMMENT 'Technical column: TRUE if this is the current version of the business key.',
    __ROW_HASH            VARCHAR(64)  COMMENT 'Technical column: SHA2 hash of tracked attributes, used to detect changes for SCD2.',
    __GOLD_LOAD_TS         TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this version row was written to GOLD.',
    CONSTRAINT PK_DIM_COMPANY PRIMARY KEY (company_hk)
)
COMMENT = '{"purpose": "Client company dimension with SCD Type-2 history.", "grain": "one row per company per changed version", "layer": "gold", "scd_type": 2}';

-- 4. DIM_EMPLOYEE -------------------------------------------------------------------
-- Depends on DIM_DEPARTMENT and DIM_OFFICE (current hash keys) and is self-referencing
-- via manager_hk -> employee_hk.
CREATE TABLE IF NOT EXISTS DIM_EMPLOYEE (
    employee_hk          VARCHAR(64)  NOT NULL COMMENT 'Surrogate hash key: SHA2-256(employee_id). Primary key of this dimension.',
    employee_id          NUMBER(4,0)  NOT NULL COMMENT 'Business key for the employee (natural key from source).',
    access_id            VARCHAR(30)  COMMENT 'Badge/access-system identifier used to join FACT_EMPLOYEE_DAILY_ACCESS.',
    employee_name        VARCHAR(300) COMMENT 'Employee full name.',
    employee_email       VARCHAR(300) COMMENT 'Employee corporate email address.',
    department_hk        VARCHAR(64)  COMMENT 'FK to DIM_DEPARTMENT.department_hk (current department version at load time).',
    office_hk             VARCHAR(64)  COMMENT 'FK to DIM_OFFICE.office_hk (current office version at load time).',
    manager_employee_hk   VARCHAR(64)  COMMENT 'FK to DIM_EMPLOYEE.employee_hk for the reporting manager (self-referencing); null for top-level roles.',
    job_title             VARCHAR(300) COMMENT 'Employee job title.',
    job_level             VARCHAR(50)  COMMENT 'Employee job level/band, e.g. Director, Manager.',
    employment_status     VARCHAR(50)  COMMENT 'Current employment status, e.g. Active, Terminated.',
    hire_date              DATE        COMMENT 'Date the employee was hired.',
    is_active              BOOLEAN     COMMENT 'Whether the employee is currently active (as of this version).',
    __EFFECTIVE_FROM_DATE DATE COMMENT 'Technical column: timestamp this dimension version became effective.',
    __EFFECTIVE_TO_DATE   DATE COMMENT 'Technical column: timestamp this dimension version was superseded; 9999-12-31 while current.',
    __IS_CURRENT          BOOLEAN      COMMENT 'Technical column: TRUE if this is the current version of the business key.',
    __ROW_HASH            VARCHAR(64)  COMMENT 'Technical column: SHA2 hash of tracked attributes, used to detect changes for SCD2.',
    __GOLD_LOAD_TS         TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this version row was written to GOLD.',
    CONSTRAINT PK_DIM_EMPLOYEE PRIMARY KEY (employee_hk),
    CONSTRAINT FK_DIM_EMPLOYEE_DEPARTMENT FOREIGN KEY (department_hk) REFERENCES DIM_DEPARTMENT (department_hk),
    CONSTRAINT FK_DIM_EMPLOYEE_OFFICE FOREIGN KEY (office_hk) REFERENCES DIM_OFFICE (office_hk),
    CONSTRAINT FK_DIM_EMPLOYEE_MANAGER FOREIGN KEY (manager_employee_hk) REFERENCES DIM_EMPLOYEE (employee_hk)
)
COMMENT = '{"purpose": "Employee dimension with SCD Type-2 history and self-referencing manager hierarchy.", "grain": "one row per employee per changed version", "layer": "gold", "scd_type": 2}';

-- 5. DIM_PROJECT ---------------------------------------------------------------------
-- Depends on DIM_COMPANY and DIM_DEPARTMENT (current hash keys).
CREATE TABLE IF NOT EXISTS DIM_PROJECT (
    project_hk             VARCHAR(64)  NOT NULL COMMENT 'Surrogate hash key: SHA2-256(project_id). Primary key of this dimension.',
    project_id             NUMBER(5,0)  NOT NULL COMMENT 'Business key for the project (natural key from source).',
    project_name           VARCHAR(400) COMMENT 'Project name.',
    company_hk              VARCHAR(64) COMMENT 'FK to DIM_COMPANY.company_hk (current company version at load time) - the client the project is delivered for.',
    owning_department_hk    VARCHAR(64) COMMENT 'FK to DIM_DEPARTMENT.department_hk (current department version at load time) owning the project.',
    project_type           VARCHAR(100) COMMENT 'Type of engagement, e.g. Modernization, Managed Services.',
    project_billing_type   VARCHAR(50)  COMMENT 'Billing arrangement, e.g. Fixed Bid, Time & Materials.',
    project_budget_usd     NUMBER(12,2) COMMENT 'Approved project budget in USD.',
    project_status         VARCHAR(50)  COMMENT 'Current project status, e.g. Active, Completed.',
    start_date              DATE        COMMENT 'Project start date.',
    planned_end_date        DATE        COMMENT 'Planned project end date.',
    actual_end_date         DATE        COMMENT 'Actual project end date; null while project is open.',
    is_active               BOOLEAN     COMMENT 'Whether the project is currently active (as of this version).',
    __EFFECTIVE_FROM_DATE DATE COMMENT 'Technical column: timestamp this dimension version became effective.',
    __EFFECTIVE_TO_DATE   DATE COMMENT 'Technical column: timestamp this dimension version was superseded; 9999-12-31 while current.',
    __IS_CURRENT          BOOLEAN      COMMENT 'Technical column: TRUE if this is the current version of the business key.',
    __ROW_HASH            VARCHAR(64)  COMMENT 'Technical column: SHA2 hash of tracked attributes, used to detect changes for SCD2.',
    __GOLD_LOAD_TS         TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this version row was written to GOLD.',
    CONSTRAINT PK_DIM_PROJECT PRIMARY KEY (project_hk),
    CONSTRAINT FK_DIM_PROJECT_COMPANY FOREIGN KEY (company_hk) REFERENCES DIM_COMPANY (company_hk),
    CONSTRAINT FK_DIM_PROJECT_DEPARTMENT FOREIGN KEY (owning_department_hk) REFERENCES DIM_DEPARTMENT (department_hk)
)
CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_hk         VARCHAR(64) NOT NULL COMMENT 'Surrogate hash key: SHA2-256(date_day). Primary key of this dimension.',
    date_day        DATE        NOT NULL COMMENT 'Business key: the calendar date (one row per date).',
    day_of_week     NUMBER(1,0) COMMENT 'ISO day of week, 1=Monday .. 7=Sunday.',
    day_name        VARCHAR(10) COMMENT 'Day name, e.g. Monday.',
    day_of_month    NUMBER(2,0) COMMENT 'Day number within the month (1-31).',
    week_of_year    NUMBER(2,0) COMMENT 'ISO week number within the year.',
    month_num       NUMBER(2,0) COMMENT 'Month number within the year (1-12).',
    month_name      VARCHAR(10) COMMENT 'Month name, e.g. January.',
    quarter_num     NUMBER(1,0) COMMENT 'Quarter number within the year (1-4).',
    year_num        NUMBER(4,0) COMMENT 'Calendar year.',
    is_business_day BOOLEAN     COMMENT 'TRUE for Monday-Friday; FALSE for Saturday/Sunday. No holiday calendar applied.',
    __GOLD_LOAD_TS  TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this row was written to GOLD.',
    CONSTRAINT PK_DIM_DATE PRIMARY KEY (date_hk)
)
COMMENT = '{"purpose": "Conformed calendar dimension covering the observed business-date range.", "grain": "one row per calendar date", "layer": "gold", "scd_type": 0}';

INSERT INTO DIM_DATE
    (date_hk, date_day, day_of_week, day_name, day_of_month, week_of_year, month_num, month_name, quarter_num, year_num, is_business_day, __GOLD_LOAD_TS)
SELECT
    SHA2(TO_VARCHAR(d.date_day), 256),
    d.date_day,
    DAYOFWEEKISO(d.date_day),
    DAYNAME(d.date_day),
    DAY(d.date_day),
    WEEKOFYEAR(d.date_day),
    MONTH(d.date_day),
    MONTHNAME(d.date_day),
    QUARTER(d.date_day),
    YEAR(d.date_day),
    DAYOFWEEKISO(d.date_day) <= 5,
    CURRENT_TIMESTAMP()
FROM (
    SELECT DATEADD('day', SEQ4(), '2018-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))  -- 2018-01-01 .. 2028-12-31 inclusive
) d
WHERE NOT EXISTS (SELECT 1 FROM DIM_DATE existing WHERE existing.date_day = d.date_day);
CREATE TABLE IF NOT EXISTS DIM_SKILL (
    skill_hk       VARCHAR(64)  NOT NULL COMMENT 'Surrogate hash key: SHA2-256(skill_id). Primary key of this dimension.',
    skill_id       NUMBER(5,0)  NOT NULL COMMENT 'Business key for the controlled skill (natural key from source).',
    skill_name     VARCHAR(200) COMMENT 'Skill display name, e.g. dbt, Snowflake.',
    skill_category VARCHAR(100) COMMENT 'Skill grouping, e.g. Data, Web Development.',
    is_active       BOOLEAN     COMMENT 'Whether the skill is currently active.',
    __ROW_HASH      VARCHAR(64) COMMENT 'Technical column: SHA2 hash of tracked attributes, used to detect changes for Type 1 upsert.',
    __GOLD_LOAD_TS  TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this row was last written/updated in GOLD.',
    CONSTRAINT PK_DIM_SKILL PRIMARY KEY (skill_hk)
)
COMMENT = '{"purpose": "Controlled skill/technology dimension, Type 1 upsert-in-place (no version history).", "grain": "one row per skill (current attributes only)", "layer": "gold", "scd_type": 1}';

CREATE TABLE IF NOT EXISTS DIM_ASSIGNMENT_ROLE (
    assignment_role_hk VARCHAR(64)  NOT NULL COMMENT 'Surrogate hash key: SHA2-256(assignment_role). Primary key of this dimension.',
    assignment_role     VARCHAR(200) NOT NULL COMMENT 'Business key: the delivery role name, e.g. Core Delivery.',
    __GOLD_LOAD_TS       TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this row was written to GOLD.',
    CONSTRAINT PK_DIM_ASSIGNMENT_ROLE PRIMARY KEY (assignment_role_hk)
)
COMMENT = '{"purpose": "Static reference dimension of delivery roles observed on assignments.", "grain": "one row per distinct assignment role", "layer": "gold", "scd_type": 0}';

CREATE TABLE IF NOT EXISTS DIM_PROFICIENCY (
    proficiency_hk     VARCHAR(64)  NOT NULL COMMENT 'Surrogate hash key: SHA2-256(proficiency_level). Primary key of this dimension.',
    proficiency_level  VARCHAR(50)  NOT NULL COMMENT 'Business key: the proficiency band name, e.g. Beginner, Expert.',
    proficiency_rank   NUMBER(1,0)  COMMENT 'Ordinal rank of the band, 1=Beginner .. 4=Expert, for ordered comparisons.',
    __GOLD_LOAD_TS      TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this row was written to GOLD.',
    CONSTRAINT PK_DIM_PROFICIENCY PRIMARY KEY (proficiency_hk)
)
COMMENT = '{"purpose": "Static ordered reference dimension of proficiency bands used by skill bridges.", "grain": "one row per distinct proficiency band", "layer": "gold", "scd_type": 0}';
--COMMENT = '{"purpose": "Project dimension with SCD Type-2 history.", "grain": "one row per project per changed version", "layer": "gold", "scd_type": 2}';
CREATE TABLE IF NOT EXISTS FACT_EMPLOYEE_DAILY_ACCESS (
    access_event_hk     VARCHAR(64)  NOT NULL COMMENT 'Surrogate hash key: SHA2-256(access_event_id). Primary key of this fact.',
    access_event_id      NUMBER(7,0) NOT NULL COMMENT 'Business key for the badge access event.',
    employee_hk          VARCHAR(64) COMMENT 'FK to DIM_EMPLOYEE.employee_hk, resolved via access_id (current employee version at load time).',
    office_hk            VARCHAR(64) COMMENT 'FK to DIM_OFFICE.office_hk where the badge event occurred (current office version at load time).',
    access_date          DATE        COMMENT 'Calendar date of the badge event.',
    access_timestamp     TIMESTAMP_NTZ COMMENT 'Exact timestamp of the badge event.',
    access_event_type   VARCHAR(20)  COMMENT 'Badge event direction, e.g. IN / OUT.',
    __GOLD_LOAD_TS TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this fact row was written to GOLD.',
    CONSTRAINT PK_FACT_EMPLOYEE_DAILY_ACCESS PRIMARY KEY (access_event_hk),
    CONSTRAINT FK_FEDA_EMPLOYEE FOREIGN KEY (employee_hk) REFERENCES DIM_EMPLOYEE (employee_hk),
    CONSTRAINT FK_FEDA_OFFICE FOREIGN KEY (office_hk) REFERENCES DIM_OFFICE (office_hk)
)
COMMENT = '{"purpose": "Badge in/out event fact used for presence/utilization analysis.", "grain": "one row per badge access event", "layer": "gold", "scd_type": "none - insert-only event fact"}';

-- Derived attendance is intentionally a separate fact. The atomic event fact
-- above remains the source of truth for audit, troubleshooting, and re-pairing.
CREATE TABLE IF NOT EXISTS FCT_EMPLOYEE_DAILY_ATTENDANCE (
    employee_daily_attendance_hk VARCHAR(64) NOT NULL COMMENT 'Surrogate hash key: SHA2-256(employee_hk || access_date). One row per employee work date.',
    employee_hk                  VARCHAR(64) NOT NULL COMMENT 'FK to DIM_EMPLOYEE.employee_hk.',
    access_date                  DATE        NOT NULL COMMENT 'Business work date; Silver rejects events whose timestamp falls on a different date.',
    first_in_timestamp           TIMESTAMP_NTZ COMMENT 'Earliest IN event on the date; null when the sequence contains only OUT events.',
    last_out_timestamp           TIMESTAMP_NTZ COMMENT 'Latest OUT event on the date; null when no OUT event exists.',
    worked_minutes               NUMBER(12,0) NOT NULL COMMENT 'Sum of valid adjacent IN-to-next-OUT durations. It excludes breaks and never uses last-out minus first-in.',
    completed_pair_count         NUMBER(9,0)  NOT NULL COMMENT 'Number of valid adjacent IN/OUT pairs included in worked_minutes.',
    unmatched_in_count           NUMBER(9,0)  NOT NULL COMMENT 'IN events not immediately followed by an OUT event.',
    unmatched_out_count          NUMBER(9,0)  NOT NULL COMMENT 'OUT events not immediately preceded by an IN event.',
    is_complete_day              BOOLEAN      NOT NULL COMMENT 'True only when at least one valid pair exists and no unmatched events remain.',
    __GOLD_LOAD_TS               TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this derived daily attendance row was last calculated.',
    CONSTRAINT PK_FCT_EMPLOYEE_DAILY_ATTENDANCE PRIMARY KEY (employee_daily_attendance_hk),
    CONSTRAINT FK_FCT_EMPLOYEE_DAILY_ATTENDANCE_EMPLOYEE FOREIGN KEY (employee_hk) REFERENCES DIM_EMPLOYEE (employee_hk)
)
COMMENT = '{"purpose": "Derived employee daily attendance from ordered badge IN/OUT events.", "grain": "one row per employee per access date", "layer": "gold", "pattern": "recomputed aggregate from immutable event fact"}';
CREATE TABLE IF NOT EXISTS FCT_PROJECT_BUDGET_PLAN (
    project_budget_version_hk VARCHAR(64) NOT NULL COMMENT 'Surrogate hash key: SHA2-256(project_id || effective_from_date). Primary key of this fact - one row per budget version.',
    project_id                 NUMBER(5,0) NOT NULL COMMENT 'Business key for the project this budget version belongs to.',
    project_hk                  VARCHAR(64) COMMENT 'FK to DIM_PROJECT.project_hk (current project version at load time).',
    company_hk                  VARCHAR(64) COMMENT 'FK to DIM_COMPANY.company_hk - the client commissioning the project (current company version at load time).',
    owning_department_hk        VARCHAR(64) COMMENT 'FK to DIM_DEPARTMENT.department_hk owning the project (current department version at load time).',
    project_budget_usd          NUMBER(12,2) COMMENT 'Approved project budget in USD for this version.',
    __EFFECTIVE_FROM_DATE DATE         COMMENT 'Technical column: first business date this budget version was effective.',
    __EFFECTIVE_TO_DATE   DATE         COMMENT 'Technical column: last business date this budget version was effective; 9999-12-31 while current.',
    __IS_CURRENT          BOOLEAN      COMMENT 'Technical column: TRUE if this is the current budget version for the project.',
    __ROW_HASH            VARCHAR(64)  COMMENT 'Technical column: SHA2 hash of the budget amount, used to detect changes.',
    __GOLD_LOAD_TS         TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this version row was written to GOLD.',
    CONSTRAINT PK_FCT_PROJECT_BUDGET_PLAN PRIMARY KEY (project_budget_version_hk),
    CONSTRAINT FK_FPBP_PROJECT FOREIGN KEY (project_hk) REFERENCES DIM_PROJECT (project_hk),
    CONSTRAINT FK_FPBP_COMPANY FOREIGN KEY (company_hk) REFERENCES DIM_COMPANY (company_hk),
    CONSTRAINT FK_FPBP_DEPARTMENT FOREIGN KEY (owning_department_hk) REFERENCES DIM_DEPARTMENT (department_hk)
)
COMMENT = '{"purpose": "Project budget fact, split out of DIM_PROJECT so budget is never duplicated via fan-out when joined through assignments or skill requirements.", "grain": "one row per project budget version", "layer": "gold", "pattern": "effective-dated financial fact"}';
CREATE TABLE IF NOT EXISTS FACT_EMPLOYEE_PROJECT_ASSIGNMENT (
    assignment_version_hk   VARCHAR(64)  NOT NULL COMMENT 'Surrogate hash key: SHA2-256(assignment_id || effective_from_date). Primary key of this fact - one row per assignment version.',
    assignment_id            NUMBER(5,0) NOT NULL COMMENT 'Business key for the assignment record (repeats across versions).',
    employee_hk              VARCHAR(64) COMMENT 'FK to DIM_EMPLOYEE.employee_hk (current employee version at load time).',
    project_hk                VARCHAR(64) COMMENT 'FK to DIM_PROJECT.project_hk (current project version at load time).',
    assignment_role_hk        VARCHAR(64) COMMENT 'FK to DIM_ASSIGNMENT_ROLE.assignment_role_hk.',
    allocation_percent      NUMBER(3,0)  COMMENT 'Percentage of employee time allocated to the project for this version.',
    assignment_start_date    DATE        COMMENT 'Date the assignment started (as of this version).',
    assignment_end_date      DATE        COMMENT 'Date the assignment ended; null while assignment is active (as of this version).',
    __EFFECTIVE_FROM_DATE DATE         COMMENT 'Technical column: first business date this assignment version was effective.',
    __EFFECTIVE_TO_DATE   DATE         COMMENT 'Technical column: last business date this assignment version was effective; 9999-12-31 while current.',
    __IS_CURRENT          BOOLEAN      COMMENT 'Technical column: TRUE if this is the current version of the assignment_id.',
    __ROW_HASH            VARCHAR(64)  COMMENT 'Technical column: SHA2 hash of tracked attributes, used to detect changes.',
    __GOLD_LOAD_TS         TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this version row was written to GOLD.',
    CONSTRAINT PK_FACT_EMPLOYEE_PROJECT_ASSIGNMENT PRIMARY KEY (assignment_version_hk),
    CONSTRAINT FK_FEPA_EMPLOYEE FOREIGN KEY (employee_hk) REFERENCES DIM_EMPLOYEE (employee_hk),
    CONSTRAINT FK_FEPA_PROJECT FOREIGN KEY (project_hk) REFERENCES DIM_PROJECT (project_hk),
    CONSTRAINT FK_FEPA_ROLE FOREIGN KEY (assignment_role_hk) REFERENCES DIM_ASSIGNMENT_ROLE (assignment_role_hk)
)
COMMENT = '{"purpose": "Employee-to-project staffing fact with effective-dated version history.", "grain": "one row per employee/project assignment version", "layer": "gold", "pattern": "effective-dated fact"}';

-- 2. BR_EMPLOYEE_SKILL (effective-dated factless bridge) -----------------------
CREATE TABLE IF NOT EXISTS BR_EMPLOYEE_SKILL (
    employee_skill_version_hk VARCHAR(64) NOT NULL COMMENT 'Surrogate hash key: SHA2-256(employee_skill_id || effective_from_date). Primary key of this bridge - one row per relationship version.',
    employee_skill_id          NUMBER(6,0) NOT NULL COMMENT 'Business key for the employee-skill relationship record (repeats across versions).',
    employee_hk                 VARCHAR(64) COMMENT 'FK to DIM_EMPLOYEE.employee_hk (current employee version at load time).',
    skill_hk                     VARCHAR(64) COMMENT 'FK to DIM_SKILL.skill_hk.',
    proficiency_hk                VARCHAR(64) COMMENT 'FK to DIM_PROFICIENCY.proficiency_hk.',
    is_primary_skill            BOOLEAN     COMMENT 'Whether this was the employee''s primary skill as of this version.',
    __EFFECTIVE_FROM_DATE DATE         COMMENT 'Technical column: first business date this relationship version was effective.',
    __EFFECTIVE_TO_DATE   DATE         COMMENT 'Technical column: last business date this relationship version was effective; 9999-12-31 while current.',
    __IS_CURRENT          BOOLEAN      COMMENT 'Technical column: TRUE if this is the current version of the employee_skill_id.',
    __ROW_HASH            VARCHAR(64)  COMMENT 'Technical column: SHA2 hash of tracked attributes, used to detect changes.',
    __GOLD_LOAD_TS         TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this version row was written to GOLD.',
    CONSTRAINT PK_BR_EMPLOYEE_SKILL PRIMARY KEY (employee_skill_version_hk),
    CONSTRAINT FK_BES_EMPLOYEE FOREIGN KEY (employee_hk) REFERENCES DIM_EMPLOYEE (employee_hk),
    CONSTRAINT FK_BES_SKILL FOREIGN KEY (skill_hk) REFERENCES DIM_SKILL (skill_hk),
    CONSTRAINT FK_BES_PROFICIENCY FOREIGN KEY (proficiency_hk) REFERENCES DIM_PROFICIENCY (proficiency_hk)
)
COMMENT = '{"purpose": "Employee-to-skill proficiency factless bridge with effective-dated version history.", "grain": "one row per employee/skill relationship version", "layer": "gold", "pattern": "effective-dated factless bridge"}';

-- 3. BR_PROJECT_SKILL_REQUIREMENT (effective-dated factless bridge) ------------
CREATE TABLE IF NOT EXISTS BR_PROJECT_SKILL_REQUIREMENT (
    project_technology_version_hk VARCHAR(64) NOT NULL COMMENT 'Surrogate hash key: SHA2-256(project_technology_id || effective_from_date). Primary key of this bridge - one row per relationship version.',
    project_technology_id          NUMBER(6,0) NOT NULL COMMENT 'Business key for the project-technology requirement record (repeats across versions).',
    project_hk                      VARCHAR(64) COMMENT 'FK to DIM_PROJECT.project_hk (current project version at load time).',
    skill_hk                         VARCHAR(64) COMMENT 'FK to DIM_SKILL.skill_hk.',
    proficiency_hk                    VARCHAR(64) COMMENT 'FK to DIM_PROFICIENCY.proficiency_hk (the required proficiency level).',
    is_primary_technology           BOOLEAN     COMMENT 'Whether this was a primary technology requirement for the project as of this version.',
    __EFFECTIVE_FROM_DATE DATE         COMMENT 'Technical column: first business date this requirement version was effective.',
    __EFFECTIVE_TO_DATE   DATE         COMMENT 'Technical column: last business date this requirement version was effective; 9999-12-31 while current.',
    __IS_CURRENT          BOOLEAN      COMMENT 'Technical column: TRUE if this is the current version of the project_technology_id.',
    __ROW_HASH            VARCHAR(64)  COMMENT 'Technical column: SHA2 hash of tracked attributes, used to detect changes.',
    __GOLD_LOAD_TS         TIMESTAMP_NTZ COMMENT 'Technical column: timestamp this version row was written to GOLD.',
    CONSTRAINT PK_BR_PROJECT_SKILL_REQUIREMENT PRIMARY KEY (project_technology_version_hk),
    CONSTRAINT FK_BPSR_PROJECT FOREIGN KEY (project_hk) REFERENCES DIM_PROJECT (project_hk),
    CONSTRAINT FK_BPSR_SKILL FOREIGN KEY (skill_hk) REFERENCES DIM_SKILL (skill_hk),
    CONSTRAINT FK_BPSR_PROFICIENCY FOREIGN KEY (proficiency_hk) REFERENCES DIM_PROFICIENCY (proficiency_hk)
)
COMMENT = '{"purpose": "Project-to-required-skill factless bridge with effective-dated version history.", "grain": "one row per project/skill requirement version", "layer": "gold", "pattern": "effective-dated factless bridge"}';



-- ============================================================================
-- SECTION: V007 - Silver Load Procedures (10 stream-consuming loaders)
-- ============================================================================

-- V007__silver_load_procedures.sql
-- Final Silver procedures only. Each stream read and its accepted/quarantined writes share one transaction.
-- Every SP_LOAD_SILVER_<ENTITY> follows these steps:
--   1. Write a STARTED audit/event record.
--   2. Read only INSERT records from the matching append-only Bronze stream
--      into a temporary validation set, deriving standardised business fields.
--   3. Insert valid typed rows into Silver and invalid rows, with their raw
--      payload and reason, into GOVERNANCE.QUARANTINE_LOG in one transaction.
--   4. Commit the stream offset only with those writes; then log SUCCESS, or
--      roll back and log FAILED if any step raises an exception.
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_SILVER_DEPARTMENTS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.DEPARTMENTS rows (via DEPARTMENTS_STRM) into SILVER.DEPARTMENTS with proper typing. Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_DEPARTMENTS'', ''SILVER'', ''HR_ANALYTICS.SILVER.DEPARTMENTS'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_DEPARTMENTS started.'');

    INSERT INTO HR_ANALYTICS.SILVER.DEPARTMENTS
        (department_id, department_code, department_name, added_date, updated_date, is_active,
         __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT
        department_id,
        department_code,
        department_name,
        TRY_TO_DATE(added_date),
        TRY_TO_DATE(updated_date),
        TRY_TO_BOOLEAN(is_active),
        __STG_FILE_NAME,
        __STG_FILE_ROW_NUMBER,
        __STG_LOAD_TS,
        CURRENT_TIMESTAMP()
    FROM HR_ANALYTICS.BRONZE.DEPARTMENTS_STRM
    WHERE METADATA$ACTION = ''INSERT'';

    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_DEPARTMENTS'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_DEPARTMENTS succeeded.'');
    RETURN ''SP_LOAD_SILVER_DEPARTMENTS: inserted '' || v_rows_inserted || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_DEPARTMENTS'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_DEPARTMENTS failed: '' || v_err_msg);
        RAISE;
END;
';

-- =============================================================================
-- V035__silver_enrichment_transformations.sql
--
-- Adds Silver-layer standardization, validation, and derived attributes per
-- tmp-transformation.md:
--   1. Country/location standardization (COMPANIES, OFFICES) - resolved via
--      an inline CASE statement (no reference table) covering the 9
--      countries observed in this dataset.
--   2. Email/identity normalization (EMPLOYEES)
--   3. Organisation hierarchy validation (EMPLOYEES)
--   4. Project lifecycle conformance (PROJECTS)
--   5. Assignment capacity normalization (EMPLOYEE_PROJECT_ASSIGNMENTS)
--   6. Skills/proficiency conformance (SKILLS, EMPLOYEE_SKILLS, PROJECT_TECHNOLOGIES)
--   7. Access data lightweight conformance (EMPLOYEE_DAILY_ACCESS)
--
-- All target columns for these transformations are defined directly in
-- V009__create_silver_tables.sql / V022__add_delta_columns_and_skill_silver_tables.sql
-- (embedded at CREATE TABLE time) - this migration only (re)defines the load
-- procedures that populate them. There is no reference table for country
-- lookups: the 9-country mapping is a plain CASE statement, kept in one
-- place per business rule (country/currency) inside each procedure that
-- needs it.
--
-- FX/currency conversion is explicitly NOT introduced here: project_budget_usd
-- is already USD, so a conversion step would add artificial complexity with
-- no current source data (project_budget_local_amount / project_currency_code
-- / budget_effective_date) to justify it. Revisit only if those source fields
-- appear.
--
-- All new validation rules are additive: existing rows in HR_ANALYTICS today
-- already satisfy every new check (0 self-managers, 0 unknown managers, all
-- countries resolvable), so this migration enriches without quarantining any
-- previously-accepted historical row.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 2. SP_LOAD_SILVER_COMPANIES -- adds country/currency standardization
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_ANALYTICS.UTIL.SP_LOAD_SILVER_COMPANIES()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.COMPANIES rows (via COMPANIES_STRM) into SILVER.COMPANIES with proper typing, delta lineage, and inline-CASE country/currency standardization; quarantines rows with a blank company_name or an unresolvable company_country. Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_quarantined INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_COMPANIES'', ''SILVER'', ''HR_ANALYTICS.SILVER.COMPANIES'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_COMPANIES started.'');

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_COMPANIES_STREAM (
        company_id NUMBER,
        company_name VARCHAR(300),
        industry VARCHAR(200),
        company_country VARCHAR(200),
        company_classification VARCHAR(50),
        added_date VARCHAR(50),
        updated_date VARCHAR(50),
        is_active VARCHAR(20),
        country_iso2 VARCHAR(2),
        country_iso3 VARCHAR(3),
        country_name_standardized VARCHAR(200),
        default_currency_code VARCHAR(3),
        __SRC_DELTA_DATE VARCHAR(50),
        __SRC_OPERATION_TYPE VARCHAR(20),
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ,
        rejection_reason VARCHAR(500)
    );
    DELETE FROM TMP_COMPANIES_STREAM;

    BEGIN TRANSACTION;

    -- Country/currency standardization: plain business-rule CASE statement,
    -- covering the 9 countries observed in this dataset. No reference table.
    INSERT INTO TMP_COMPANIES_STREAM (company_id, company_name, industry, company_country, company_classification, added_date, updated_date, is_active, country_iso2, country_iso3, country_name_standardized, default_currency_code, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, rejection_reason)
        SELECT s.company_id, s.company_name, s.industry, s.company_country, s.company_classification,
           s.added_date, s.updated_date, s.is_active,
        CASE
             WHEN s.company_country = ''Australia'' THEN ''AU''
             WHEN s.company_country = ''Canada'' THEN ''CA''
             WHEN s.company_country = ''Germany'' THEN ''DE''
             WHEN s.company_country = ''India'' THEN ''IN''
             WHEN s.company_country = ''Ireland'' THEN ''IE''
             WHEN s.company_country = ''Netherlands'' THEN ''NL''
             WHEN s.company_country = ''Singapore'' THEN ''SG''
             WHEN s.company_country = ''United Kingdom'' THEN ''GB''
             WHEN s.company_country = ''United States'' THEN ''US''
             ELSE NULL END,
        CASE
             WHEN s.company_country = ''Australia'' THEN ''AUS''
             WHEN s.company_country = ''Canada'' THEN ''CAN''
             WHEN s.company_country = ''Germany'' THEN ''DEU''
             WHEN s.company_country = ''India'' THEN ''IND''
             WHEN s.company_country = ''Ireland'' THEN ''IRL''
             WHEN s.company_country = ''Netherlands'' THEN ''NLD''
             WHEN s.company_country = ''Singapore'' THEN ''SGP''
             WHEN s.company_country = ''United Kingdom'' THEN ''GBR''
             WHEN s.company_country = ''United States'' THEN ''USA''
             ELSE NULL END,
        CASE
             WHEN s.company_country = ''Australia'' THEN ''Australia''
             WHEN s.company_country = ''Canada'' THEN ''Canada''
             WHEN s.company_country = ''Germany'' THEN ''Germany''
             WHEN s.company_country = ''India'' THEN ''India''
             WHEN s.company_country = ''Ireland'' THEN ''Ireland''
             WHEN s.company_country = ''Netherlands'' THEN ''Netherlands''
             WHEN s.company_country = ''Singapore'' THEN ''Singapore''
             WHEN s.company_country = ''United Kingdom'' THEN ''United Kingdom''
             WHEN s.company_country = ''United States'' THEN ''United States''
             ELSE NULL END,
        CASE
             WHEN s.company_country = ''Australia'' THEN ''AUD''
             WHEN s.company_country = ''Canada'' THEN ''CAD''
             WHEN s.company_country = ''Germany'' THEN ''EUR''
             WHEN s.company_country = ''India'' THEN ''INR''
             WHEN s.company_country = ''Ireland'' THEN ''EUR''
             WHEN s.company_country = ''Netherlands'' THEN ''EUR''
             WHEN s.company_country = ''Singapore'' THEN ''SGD''
             WHEN s.company_country = ''United Kingdom'' THEN ''GBP''
             WHEN s.company_country = ''United States'' THEN ''USD''
             ELSE NULL END,
           s.__SRC_DELTA_DATE, s.__SRC_OPERATION_TYPE,
           s.__STG_FILE_NAME, s.__STG_FILE_ROW_NUMBER, s.__STG_LOAD_TS,
           CASE WHEN s.company_name IS NULL OR TRIM(s.company_name) = '''' THEN ''NULL_REQUIRED_FIELD:company_name''
                WHEN s.company_country IS NOT NULL AND s.company_country NOT IN (''Australia'',''Canada'',''Germany'',''India'',''Ireland'',''Netherlands'',''Singapore'',''United Kingdom'',''United States'') THEN ''UNRESOLVED_COUNTRY:company_country''
                ELSE NULL END AS rejection_reason
    FROM HR_ANALYTICS.BRONZE.COMPANIES_STRM s
    WHERE METADATA$ACTION = ''INSERT'';

    INSERT INTO HR_ANALYTICS.SILVER.COMPANIES
        (company_id, company_name, industry, company_country, company_classification, added_date, updated_date, is_active,
         country_iso2, country_iso3, country_name_standardized, default_currency_code,
         __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT company_id, company_name, industry, company_country, company_classification,
           TRY_TO_DATE(added_date), TRY_TO_DATE(updated_date), TRY_TO_BOOLEAN(is_active),
           country_iso2, country_iso3, country_name_standardized, default_currency_code,
           TRY_TO_DATE(__SRC_DELTA_DATE), __SRC_OPERATION_TYPE,
           __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, CURRENT_TIMESTAMP()
    FROM TMP_COMPANIES_STREAM
    WHERE rejection_reason IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
        (source_layer, source_table, target_table, natural_key_value, src_delta_date, src_file_name, rejection_reason, raw_row_variant, quarantined_at)
    SELECT ''SILVER'', ''HR_ANALYTICS.BRONZE.COMPANIES'', ''HR_ANALYTICS.SILVER.COMPANIES'',
           TO_VARCHAR(company_id), TRY_TO_DATE(__SRC_DELTA_DATE), __STG_FILE_NAME, rejection_reason,
           OBJECT_CONSTRUCT(''company_id'', company_id, ''company_name'', company_name, ''industry'', industry,
                             ''company_country'', company_country, ''company_classification'', company_classification,
                             ''delta_date'', __SRC_DELTA_DATE, ''operation_type'', __SRC_OPERATION_TYPE),
           CURRENT_TIMESTAMP()
    FROM TMP_COMPANIES_STREAM
    WHERE rejection_reason IS NOT NULL;
    v_rows_quarantined := SQLROWCOUNT;

    COMMIT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_COMPANIES'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_COMPANIES succeeded.'');
    RETURN ''SP_LOAD_SILVER_COMPANIES: inserted '' || v_rows_inserted || '' row(s), quarantined '' || v_rows_quarantined || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_COMPANIES'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_COMPANIES failed: '' || v_err_msg);
        RAISE;
END;
';

-- -----------------------------------------------------------------------------
-- 3. SP_LOAD_SILVER_OFFICES -- adds country/currency standardization + region validity
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_ANALYTICS.UTIL.SP_LOAD_SILVER_OFFICES()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.OFFICES rows (via OFFICES_STRM) into SILVER.OFFICES with proper typing and inline-CASE country/currency standardization; flags is_region_valid against the known APAC/EU/NA region set and quarantines rows with an unresolvable office_country. Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_quarantined INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_OFFICES'', ''SILVER'', ''HR_ANALYTICS.SILVER.OFFICES'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_OFFICES started.'');

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_OFFICES_STREAM (
        office_id NUMBER,
        office_code VARCHAR(50),
        office_city VARCHAR(200),
        office_country VARCHAR(200),
        office_region VARCHAR(20),
        added_date VARCHAR(50),
        updated_date VARCHAR(50),
        is_active VARCHAR(20),
        country_iso2 VARCHAR(2),
        country_iso3 VARCHAR(3),
        country_name_standardized VARCHAR(200),
        default_currency_code VARCHAR(3),
        is_region_valid BOOLEAN,
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ,
        rejection_reason VARCHAR(500)
    );
    DELETE FROM TMP_OFFICES_STREAM;

    BEGIN TRANSACTION;

    -- Country/currency standardization: plain business-rule CASE statement,
    -- covering the 9 countries observed in this dataset. No reference table.
    INSERT INTO TMP_OFFICES_STREAM (office_id, office_code, office_city, office_country, office_region, added_date, updated_date, is_active, country_iso2, country_iso3, country_name_standardized, default_currency_code, is_region_valid, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, rejection_reason)
        SELECT s.office_id, s.office_code, s.office_city, s.office_country, s.office_region,
           s.added_date, s.updated_date, s.is_active,
        CASE
             WHEN s.office_country = ''Australia'' THEN ''AU''
             WHEN s.office_country = ''Canada'' THEN ''CA''
             WHEN s.office_country = ''Germany'' THEN ''DE''
             WHEN s.office_country = ''India'' THEN ''IN''
             WHEN s.office_country = ''Ireland'' THEN ''IE''
             WHEN s.office_country = ''Netherlands'' THEN ''NL''
             WHEN s.office_country = ''Singapore'' THEN ''SG''
             WHEN s.office_country = ''United Kingdom'' THEN ''GB''
             WHEN s.office_country = ''United States'' THEN ''US''
             ELSE NULL END,
        CASE
             WHEN s.office_country = ''Australia'' THEN ''AUS''
             WHEN s.office_country = ''Canada'' THEN ''CAN''
             WHEN s.office_country = ''Germany'' THEN ''DEU''
             WHEN s.office_country = ''India'' THEN ''IND''
             WHEN s.office_country = ''Ireland'' THEN ''IRL''
             WHEN s.office_country = ''Netherlands'' THEN ''NLD''
             WHEN s.office_country = ''Singapore'' THEN ''SGP''
             WHEN s.office_country = ''United Kingdom'' THEN ''GBR''
             WHEN s.office_country = ''United States'' THEN ''USA''
             ELSE NULL END,
        CASE
             WHEN s.office_country = ''Australia'' THEN ''Australia''
             WHEN s.office_country = ''Canada'' THEN ''Canada''
             WHEN s.office_country = ''Germany'' THEN ''Germany''
             WHEN s.office_country = ''India'' THEN ''India''
             WHEN s.office_country = ''Ireland'' THEN ''Ireland''
             WHEN s.office_country = ''Netherlands'' THEN ''Netherlands''
             WHEN s.office_country = ''Singapore'' THEN ''Singapore''
             WHEN s.office_country = ''United Kingdom'' THEN ''United Kingdom''
             WHEN s.office_country = ''United States'' THEN ''United States''
             ELSE NULL END,
        CASE
             WHEN s.office_country = ''Australia'' THEN ''AUD''
             WHEN s.office_country = ''Canada'' THEN ''CAD''
             WHEN s.office_country = ''Germany'' THEN ''EUR''
             WHEN s.office_country = ''India'' THEN ''INR''
             WHEN s.office_country = ''Ireland'' THEN ''EUR''
             WHEN s.office_country = ''Netherlands'' THEN ''EUR''
             WHEN s.office_country = ''Singapore'' THEN ''SGD''
             WHEN s.office_country = ''United Kingdom'' THEN ''GBP''
             WHEN s.office_country = ''United States'' THEN ''USD''
             ELSE NULL END,
           CASE WHEN s.office_region IN (''APAC'', ''EU'', ''NA'') THEN TRUE ELSE FALSE END,
           s.__STG_FILE_NAME, s.__STG_FILE_ROW_NUMBER, s.__STG_LOAD_TS,
           CASE WHEN s.office_country IS NOT NULL AND s.office_country NOT IN (''Australia'',''Canada'',''Germany'',''India'',''Ireland'',''Netherlands'',''Singapore'',''United Kingdom'',''United States'') THEN ''UNRESOLVED_COUNTRY:office_country''
                ELSE NULL END AS rejection_reason
    FROM HR_ANALYTICS.BRONZE.OFFICES_STRM s
    WHERE METADATA$ACTION = ''INSERT'';

    INSERT INTO HR_ANALYTICS.SILVER.OFFICES
        (office_id, office_code, office_city, office_country, office_region, added_date, updated_date, is_active,
         country_iso2, country_iso3, country_name_standardized, default_currency_code, is_region_valid,
         __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT
        office_id, office_code, office_city, office_country, office_region,
        TRY_TO_DATE(added_date), TRY_TO_DATE(updated_date), TRY_TO_BOOLEAN(is_active),
        country_iso2, country_iso3, country_name_standardized, default_currency_code, is_region_valid,
        __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, CURRENT_TIMESTAMP()
    FROM TMP_OFFICES_STREAM
    WHERE rejection_reason IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
        (source_layer, source_table, target_table, natural_key_value, src_delta_date, src_file_name, rejection_reason, raw_row_variant, quarantined_at)
    SELECT ''SILVER'', ''HR_ANALYTICS.BRONZE.OFFICES'', ''HR_ANALYTICS.SILVER.OFFICES'',
           TO_VARCHAR(office_id), NULL, __STG_FILE_NAME, rejection_reason,
           OBJECT_CONSTRUCT(''office_id'', office_id, ''office_code'', office_code, ''office_country'', office_country,
                             ''office_region'', office_region),
           CURRENT_TIMESTAMP()
    FROM TMP_OFFICES_STREAM
    WHERE rejection_reason IS NOT NULL;
    v_rows_quarantined := SQLROWCOUNT;

    COMMIT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_OFFICES'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_OFFICES succeeded.'');
    RETURN ''SP_LOAD_SILVER_OFFICES: inserted '' || v_rows_inserted || '' row(s), quarantined '' || v_rows_quarantined || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_OFFICES'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_OFFICES failed: '' || v_err_msg);
        RAISE;
END;
';
-- -----------------------------------------------------------------------------
-- 4. SP_LOAD_SILVER_EMPLOYEES -- adds email normalization + org hierarchy validation
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_ANALYTICS.UTIL.SP_LOAD_SILVER_EMPLOYEES()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.EMPLOYEES rows (via EMPLOYEES_STRM) into SILVER.EMPLOYEES with proper typing, delta lineage, email/name normalization, and org hierarchy validation flags; quarantines rows with a blank employee_name, an unresolvable department_id/office_id, a self-referencing manager_employee_id, or an unresolvable manager_employee_id. Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_quarantined INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_EMPLOYEES'', ''SILVER'', ''HR_ANALYTICS.SILVER.EMPLOYEES'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_EMPLOYEES started.'');

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_EMPLOYEES_RAW (
        employee_id NUMBER,
        access_id VARCHAR(30),
        employee_name VARCHAR(300),
        employee_email VARCHAR(300),
        department_id NUMBER,
        office_id NUMBER,
        manager_employee_id NUMBER,
        job_title VARCHAR(300),
        job_level VARCHAR(50),
        employment_status VARCHAR(50),
        hire_date VARCHAR(50),
        added_date VARCHAR(50),
        updated_date VARCHAR(50),
        is_active VARCHAR(20),
        __SRC_DELTA_DATE VARCHAR(50),
        __SRC_OPERATION_TYPE VARCHAR(20),
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ
    );
    DELETE FROM TMP_EMPLOYEES_RAW;

    INSERT INTO TMP_EMPLOYEES_RAW
        SELECT employee_id, access_id, employee_name, employee_email, department_id, office_id, manager_employee_id,
               job_title, job_level, employment_status, hire_date, added_date, updated_date, is_active,
               __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS
        FROM HR_ANALYTICS.BRONZE.EMPLOYEES_STRM
        WHERE METADATA$ACTION = ''INSERT'';

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_EMPLOYEES_STREAM (
        employee_id NUMBER,
        access_id VARCHAR(30),
        employee_name VARCHAR(300),
        employee_email VARCHAR(300),
        department_id NUMBER,
        office_id NUMBER,
        manager_employee_id NUMBER,
        job_title VARCHAR(300),
        job_level VARCHAR(50),
        employment_status VARCHAR(50),
        hire_date VARCHAR(50),
        added_date VARCHAR(50),
        updated_date VARCHAR(50),
        is_active VARCHAR(20),
        email_domain VARCHAR(200),
        is_email_domain_valid BOOLEAN,
        employee_name_normalized VARCHAR(300),
        is_manager_reference_valid BOOLEAN,
        is_self_manager BOOLEAN,
        is_org_root BOOLEAN,
        is_department_manager_match BOOLEAN,
        __SRC_DELTA_DATE VARCHAR(50),
        __SRC_OPERATION_TYPE VARCHAR(20),
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ,
        rejection_reason VARCHAR(500)
    );
    DELETE FROM TMP_EMPLOYEES_STREAM;

    BEGIN TRANSACTION;

    INSERT INTO TMP_EMPLOYEES_STREAM (employee_id, access_id, employee_name, employee_email, department_id, office_id, manager_employee_id, job_title, job_level, employment_status, hire_date, added_date, updated_date, is_active, email_domain, is_email_domain_valid, employee_name_normalized, is_manager_reference_valid, is_self_manager, is_org_root, is_department_manager_match, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, rejection_reason)
        SELECT s.employee_id, s.access_id, s.employee_name, s.employee_email, s.department_id, s.office_id, s.manager_employee_id,
           s.job_title, s.job_level, s.employment_status, s.hire_date, s.added_date, s.updated_date, s.is_active,
           LOWER(TRIM(SPLIT_PART(s.employee_email, ''@'', 2))) AS email_domain,
           CASE WHEN s.employee_email IS NULL THEN NULL
                WHEN LOWER(TRIM(SPLIT_PART(s.employee_email, ''@'', 2))) = ''des-dbt.com'' THEN TRUE
                ELSE FALSE END AS is_email_domain_valid,
           INITCAP(TRIM(REGEXP_REPLACE(s.employee_name, ''\s+'', '' ''))) AS employee_name_normalized,
           -- Manager existence is checked against SILVER (already-committed) OR the
           -- same incoming batch (TMP_EMPLOYEES_RAW) so a fresh full load where a
           -- manager and their reports arrive in the same batch does not incorrectly
           -- quarantine every report as "manager not found".
           CASE WHEN s.manager_employee_id IS NULL THEN NULL
                WHEN s.manager_employee_id = s.employee_id THEN FALSE
                WHEN mgr.employee_id IS NULL AND mgr_batch.employee_id IS NULL THEN FALSE
                ELSE TRUE END AS is_manager_reference_valid,
           CASE WHEN s.manager_employee_id = s.employee_id THEN TRUE ELSE FALSE END AS is_self_manager,
           CASE WHEN s.manager_employee_id IS NULL THEN TRUE ELSE FALSE END AS is_org_root,
           CASE WHEN s.manager_employee_id IS NULL THEN NULL
                ELSE (COALESCE(mgr.department_id, mgr_batch.department_id) = s.department_id) END AS is_department_manager_match,
           s.__SRC_DELTA_DATE, s.__SRC_OPERATION_TYPE, s.__STG_FILE_NAME, s.__STG_FILE_ROW_NUMBER, s.__STG_LOAD_TS,
           CASE WHEN s.employee_name IS NULL OR TRIM(s.employee_name) = '''' THEN ''NULL_REQUIRED_FIELD:employee_name''
                WHEN s.department_id IS NOT NULL AND s.department_id NOT IN (SELECT department_id FROM HR_ANALYTICS.SILVER.DEPARTMENTS) THEN ''FK_NOT_FOUND:department_id''
                WHEN s.office_id IS NOT NULL AND s.office_id NOT IN (SELECT office_id FROM HR_ANALYTICS.SILVER.OFFICES) THEN ''FK_NOT_FOUND:office_id''
                WHEN s.manager_employee_id = s.employee_id THEN ''SELF_MANAGER_REFERENCE:manager_employee_id''
                WHEN s.manager_employee_id IS NOT NULL AND mgr.employee_id IS NULL AND mgr_batch.employee_id IS NULL THEN ''FK_NOT_FOUND:manager_employee_id''
                ELSE NULL END AS rejection_reason
    FROM TMP_EMPLOYEES_RAW s
    LEFT JOIN HR_ANALYTICS.SILVER.EMPLOYEES mgr ON mgr.employee_id = s.manager_employee_id
    LEFT JOIN TMP_EMPLOYEES_RAW mgr_batch ON mgr_batch.employee_id = s.manager_employee_id;

    INSERT INTO HR_ANALYTICS.SILVER.EMPLOYEES
        (employee_id, access_id, employee_name, employee_email, department_id, office_id, manager_employee_id,
         job_title, job_level, employment_status, hire_date, added_date, updated_date, is_active,
         email_domain, is_email_domain_valid, employee_name_normalized,
         is_manager_reference_valid, is_self_manager, is_org_root, is_department_manager_match,
         __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT employee_id, access_id, employee_name, employee_email, department_id, office_id, manager_employee_id,
           job_title, job_level, employment_status,
           TRY_TO_DATE(hire_date), TRY_TO_DATE(added_date), TRY_TO_DATE(updated_date), TRY_TO_BOOLEAN(is_active),
           email_domain, is_email_domain_valid, employee_name_normalized,
           is_manager_reference_valid, is_self_manager, is_org_root, is_department_manager_match,
           TRY_TO_DATE(__SRC_DELTA_DATE), __SRC_OPERATION_TYPE,
           __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, CURRENT_TIMESTAMP()
    FROM TMP_EMPLOYEES_STREAM
    WHERE rejection_reason IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
        (source_layer, source_table, target_table, natural_key_value, src_delta_date, src_file_name, rejection_reason, raw_row_variant, quarantined_at)
    SELECT ''SILVER'', ''HR_ANALYTICS.BRONZE.EMPLOYEES'', ''HR_ANALYTICS.SILVER.EMPLOYEES'',
           TO_VARCHAR(employee_id), TRY_TO_DATE(__SRC_DELTA_DATE), __STG_FILE_NAME, rejection_reason,
           OBJECT_CONSTRUCT(''employee_id'', employee_id, ''employee_name'', employee_name, ''department_id'', department_id,
                             ''office_id'', office_id, ''manager_employee_id'', manager_employee_id,
                             ''delta_date'', __SRC_DELTA_DATE, ''operation_type'', __SRC_OPERATION_TYPE),
           CURRENT_TIMESTAMP()
    FROM TMP_EMPLOYEES_STREAM
    WHERE rejection_reason IS NOT NULL;
    v_rows_quarantined := SQLROWCOUNT;

    COMMIT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_EMPLOYEES'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_EMPLOYEES succeeded.'');
    RETURN ''SP_LOAD_SILVER_EMPLOYEES: inserted '' || v_rows_inserted || '' row(s), quarantined '' || v_rows_quarantined || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_EMPLOYEES'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_EMPLOYEES failed: '' || v_err_msg);
        RAISE;
END;
';

-- -----------------------------------------------------------------------------
-- 5. SP_LOAD_SILVER_PROJECTS -- adds project lifecycle conformance
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_ANALYTICS.UTIL.SP_LOAD_SILVER_PROJECTS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.PROJECTS rows (via PROJECTS_STRM) into SILVER.PROJECTS with proper typing, delta lineage, and derived project_lifecycle_state; quarantines rows with a blank project_name, an unresolvable company_id/owning_department_id, or an invalid date sequence (start after planned end, actual end before start, or Completed without an actual_end_date). Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_quarantined INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_PROJECTS'', ''SILVER'', ''HR_ANALYTICS.SILVER.PROJECTS'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_PROJECTS started.'');

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_PROJECTS_STREAM (
        project_id NUMBER,
        project_name VARCHAR(400),
        company_id NUMBER,
        owning_department_id NUMBER,
        project_type VARCHAR(100),
        project_billing_type VARCHAR(50),
        project_budget_usd NUMBER,
        project_status VARCHAR(50),
        start_date VARCHAR(50),
        planned_end_date VARCHAR(50),
        actual_end_date VARCHAR(50),
        added_date VARCHAR(50),
        updated_date VARCHAR(50),
        is_active VARCHAR(20),
        project_lifecycle_state VARCHAR(20),
        is_date_sequence_valid BOOLEAN,
        __SRC_DELTA_DATE VARCHAR(50),
        __SRC_OPERATION_TYPE VARCHAR(20),
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ,
        rejection_reason VARCHAR(500)
    );
    DELETE FROM TMP_PROJECTS_STREAM;

    BEGIN TRANSACTION;

    INSERT INTO TMP_PROJECTS_STREAM (project_id, project_name, company_id, owning_department_id, project_type, project_billing_type, project_budget_usd, project_status, start_date, planned_end_date, actual_end_date, added_date, updated_date, is_active, project_lifecycle_state, is_date_sequence_valid, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, rejection_reason)
        SELECT project_id, project_name, company_id, owning_department_id, project_type, project_billing_type,
           project_budget_usd, project_status, start_date, planned_end_date, actual_end_date, added_date, updated_date, is_active,
           CASE WHEN project_status = ''Completed'' THEN ''COMPLETED''
                WHEN project_status = ''On Hold'' THEN ''ON_HOLD''
                WHEN project_status = ''Active'' AND TRY_TO_DATE(start_date) IS NOT NULL AND TRY_TO_DATE(start_date) > CURRENT_DATE() THEN ''PLANNED''
                WHEN project_status = ''Active'' THEN ''IN_FLIGHT''
                ELSE ''UNKNOWN'' END AS project_lifecycle_state,
           CASE WHEN TRY_TO_DATE(start_date) IS NOT NULL AND TRY_TO_DATE(planned_end_date) IS NOT NULL
                     AND TRY_TO_DATE(start_date) > TRY_TO_DATE(planned_end_date) THEN FALSE
                WHEN TRY_TO_DATE(start_date) IS NOT NULL AND TRY_TO_DATE(actual_end_date) IS NOT NULL
                     AND TRY_TO_DATE(actual_end_date) < TRY_TO_DATE(start_date) THEN FALSE
                WHEN project_status = ''Completed'' AND actual_end_date IS NULL THEN FALSE
                ELSE TRUE END AS is_date_sequence_valid,
           __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS,
           CASE WHEN project_name IS NULL OR TRIM(project_name) = '''' THEN ''NULL_REQUIRED_FIELD:project_name''
                WHEN company_id IS NOT NULL AND company_id NOT IN (SELECT company_id FROM HR_ANALYTICS.SILVER.COMPANIES) THEN ''FK_NOT_FOUND:company_id''
                WHEN owning_department_id IS NOT NULL AND owning_department_id NOT IN (SELECT department_id FROM HR_ANALYTICS.SILVER.DEPARTMENTS) THEN ''FK_NOT_FOUND:owning_department_id''
                WHEN TRY_TO_DATE(start_date) IS NOT NULL AND TRY_TO_DATE(planned_end_date) IS NOT NULL
                     AND TRY_TO_DATE(start_date) > TRY_TO_DATE(planned_end_date) THEN ''INVALID_DATE_SEQUENCE:start_after_planned_end''
                WHEN TRY_TO_DATE(start_date) IS NOT NULL AND TRY_TO_DATE(actual_end_date) IS NOT NULL
                     AND TRY_TO_DATE(actual_end_date) < TRY_TO_DATE(start_date) THEN ''INVALID_DATE_SEQUENCE:actual_end_before_start''
                ELSE NULL END AS rejection_reason
    FROM HR_ANALYTICS.BRONZE.PROJECTS_STRM
    WHERE METADATA$ACTION = ''INSERT'';

    INSERT INTO HR_ANALYTICS.SILVER.PROJECTS
        (project_id, project_name, company_id, owning_department_id, project_type, project_billing_type,
         project_budget_usd, project_status, start_date, planned_end_date, actual_end_date, added_date, updated_date, is_active,
         project_lifecycle_state, is_date_sequence_valid,
         __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT project_id, project_name, company_id, owning_department_id, project_type, project_billing_type,
           project_budget_usd, project_status,
           TRY_TO_DATE(start_date), TRY_TO_DATE(planned_end_date), TRY_TO_DATE(actual_end_date),
           TRY_TO_DATE(added_date), TRY_TO_DATE(updated_date), TRY_TO_BOOLEAN(is_active),
           project_lifecycle_state, is_date_sequence_valid,
           TRY_TO_DATE(__SRC_DELTA_DATE), __SRC_OPERATION_TYPE,
           __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, CURRENT_TIMESTAMP()
    FROM TMP_PROJECTS_STREAM
    WHERE rejection_reason IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
        (source_layer, source_table, target_table, natural_key_value, src_delta_date, src_file_name, rejection_reason, raw_row_variant, quarantined_at)
    SELECT ''SILVER'', ''HR_ANALYTICS.BRONZE.PROJECTS'', ''HR_ANALYTICS.SILVER.PROJECTS'',
           TO_VARCHAR(project_id), TRY_TO_DATE(__SRC_DELTA_DATE), __STG_FILE_NAME, rejection_reason,
           OBJECT_CONSTRUCT(''project_id'', project_id, ''project_name'', project_name, ''company_id'', company_id,
                             ''owning_department_id'', owning_department_id, ''start_date'', start_date,
                             ''planned_end_date'', planned_end_date, ''actual_end_date'', actual_end_date,
                             ''delta_date'', __SRC_DELTA_DATE, ''operation_type'', __SRC_OPERATION_TYPE),
           CURRENT_TIMESTAMP()
    FROM TMP_PROJECTS_STREAM
    WHERE rejection_reason IS NOT NULL;
    v_rows_quarantined := SQLROWCOUNT;

    COMMIT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_PROJECTS'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_PROJECTS succeeded.'');
    RETURN ''SP_LOAD_SILVER_PROJECTS: inserted '' || v_rows_inserted || '' row(s), quarantined '' || v_rows_quarantined || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_PROJECTS'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_PROJECTS failed: '' || v_err_msg);
        RAISE;
END;
';

-- -----------------------------------------------------------------------------
-- 6. SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS -- adds capacity normalization
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_ANALYTICS.UTIL.SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS rows (via stream) into SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS with proper typing, delta lineage, and allocation_fraction/is_active_assignment derivation; quarantines rows with a blank employee_id/project_id, an unresolvable employee_id/project_id, an allocation_percent outside 0-100, or an assignment_start_date after assignment_end_date. Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_quarantined INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS'', ''SILVER'', ''HR_ANALYTICS.SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS started.'');

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_ASSIGNMENTS_STREAM (
        assignment_id NUMBER,
        employee_id NUMBER,
        project_id NUMBER,
        assignment_role VARCHAR(200),
        allocation_percent NUMBER,
        assignment_start_date VARCHAR(50),
        assignment_end_date VARCHAR(50),
        allocation_fraction NUMBER(6,4),
        is_active_assignment BOOLEAN,
        is_allocation_valid BOOLEAN,
        __SRC_DELTA_DATE VARCHAR(50),
        __SRC_OPERATION_TYPE VARCHAR(20),
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ,
        rejection_reason VARCHAR(500)
    );
    DELETE FROM TMP_ASSIGNMENTS_STREAM;

    BEGIN TRANSACTION;

    INSERT INTO TMP_ASSIGNMENTS_STREAM (assignment_id, employee_id, project_id, assignment_role, allocation_percent, assignment_start_date, assignment_end_date, allocation_fraction, is_active_assignment, is_allocation_valid, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, rejection_reason)
        SELECT assignment_id, employee_id, project_id, assignment_role, allocation_percent, assignment_start_date, assignment_end_date,
           CASE WHEN allocation_percent IS NULL THEN NULL ELSE allocation_percent / 100.0 END AS allocation_fraction,
           CASE WHEN assignment_end_date IS NULL THEN TRUE
                WHEN TRY_TO_DATE(assignment_end_date) IS NOT NULL AND TRY_TO_DATE(assignment_end_date) >= CURRENT_DATE() THEN TRUE
                ELSE FALSE END AS is_active_assignment,
           CASE WHEN allocation_percent IS NULL THEN NULL
                WHEN allocation_percent < 0 OR allocation_percent > 100 THEN FALSE
                ELSE TRUE END AS is_allocation_valid,
           __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS,
           CASE WHEN employee_id IS NULL THEN ''NULL_REQUIRED_FIELD:employee_id''
                WHEN project_id IS NULL THEN ''NULL_REQUIRED_FIELD:project_id''
                WHEN employee_id NOT IN (SELECT employee_id FROM HR_ANALYTICS.SILVER.EMPLOYEES) THEN ''FK_NOT_FOUND:employee_id''
                WHEN project_id NOT IN (SELECT project_id FROM HR_ANALYTICS.SILVER.PROJECTS) THEN ''FK_NOT_FOUND:project_id''
                WHEN allocation_percent IS NOT NULL AND (allocation_percent < 0 OR allocation_percent > 100) THEN ''INVALID_ALLOCATION_PERCENT''
                WHEN TRY_TO_DATE(assignment_start_date) IS NOT NULL AND TRY_TO_DATE(assignment_end_date) IS NOT NULL
                     AND TRY_TO_DATE(assignment_start_date) > TRY_TO_DATE(assignment_end_date) THEN ''INVALID_DATE_SEQUENCE:start_after_end''
                ELSE NULL END AS rejection_reason
    FROM HR_ANALYTICS.BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS_STRM
    WHERE METADATA$ACTION = ''INSERT'';

    INSERT INTO HR_ANALYTICS.SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS
        (assignment_id, employee_id, project_id, assignment_role, allocation_percent, assignment_start_date, assignment_end_date,
         allocation_fraction, is_active_assignment, is_allocation_valid,
         __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT assignment_id, employee_id, project_id, assignment_role, allocation_percent,
           TRY_TO_DATE(assignment_start_date), TRY_TO_DATE(assignment_end_date),
           allocation_fraction, is_active_assignment, is_allocation_valid,
           TRY_TO_DATE(__SRC_DELTA_DATE), __SRC_OPERATION_TYPE,
           __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, CURRENT_TIMESTAMP()
    FROM TMP_ASSIGNMENTS_STREAM
    WHERE rejection_reason IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
        (source_layer, source_table, target_table, natural_key_value, src_delta_date, src_file_name, rejection_reason, raw_row_variant, quarantined_at)
    SELECT ''SILVER'', ''HR_ANALYTICS.BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS'', ''HR_ANALYTICS.SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS'',
           TO_VARCHAR(assignment_id), TRY_TO_DATE(__SRC_DELTA_DATE), __STG_FILE_NAME, rejection_reason,
           OBJECT_CONSTRUCT(''assignment_id'', assignment_id, ''employee_id'', employee_id, ''project_id'', project_id,
                             ''allocation_percent'', allocation_percent, ''delta_date'', __SRC_DELTA_DATE, ''operation_type'', __SRC_OPERATION_TYPE),
           CURRENT_TIMESTAMP()
    FROM TMP_ASSIGNMENTS_STREAM
    WHERE rejection_reason IS NOT NULL;
    v_rows_quarantined := SQLROWCOUNT;

    COMMIT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS succeeded.'');
    RETURN ''SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS: inserted '' || v_rows_inserted || '' row(s), quarantined '' || v_rows_quarantined || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS failed: '' || v_err_msg);
        RAISE;
END;
';

-- -----------------------------------------------------------------------------
-- 7. SP_LOAD_SILVER_SKILLS -- adds canonical skill_code
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_ANALYTICS.UTIL.SP_LOAD_SILVER_SKILLS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.SKILLS rows (via SKILLS_STRM) into SILVER.SKILLS with proper typing and a canonical skill_code; quarantines rows with a blank skill_name. Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_quarantined INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_SKILLS'', ''SILVER'', ''HR_ANALYTICS.SILVER.SKILLS'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_SKILLS started.'');

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_SKILLS_STREAM (
        skill_id NUMBER,
        skill_name VARCHAR(200),
        skill_category VARCHAR(100),
        added_date VARCHAR(50),
        updated_date VARCHAR(50),
        is_active VARCHAR(20),
        skill_code VARCHAR(50),
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ,
        rejection_reason VARCHAR(500)
    );
    DELETE FROM TMP_SKILLS_STREAM;

    BEGIN TRANSACTION;

    INSERT INTO TMP_SKILLS_STREAM (skill_id, skill_name, skill_category, added_date, updated_date, is_active, skill_code, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, rejection_reason)
        SELECT skill_id, skill_name, skill_category, added_date, updated_date, is_active,
        CASE
            WHEN skill_name = ''AWS'' THEN ''AWS''
            WHEN skill_name = ''Airflow'' THEN ''AIRFLOW''
            WHEN skill_name = ''Android'' THEN ''ANDROID''
            WHEN skill_name = ''Apache Kafka'' THEN ''KAFKA''
            WHEN skill_name = ''Apache Spark'' THEN ''SPARK''
            WHEN skill_name = ''Azure'' THEN ''AZURE''
            WHEN skill_name = ''Cybersecurity'' THEN ''CYBERSECURITY''
            WHEN skill_name = ''Cypress'' THEN ''CYPRESS''
            WHEN skill_name = ''Databricks'' THEN ''DATABRICKS''
            WHEN skill_name = ''Docker'' THEN ''DOCKER''
            WHEN skill_name = ''Flutter'' THEN ''FLUTTER''
            WHEN skill_name = ''Google Analytics'' THEN ''GOOGLE_ANALYTICS''
            WHEN skill_name = ''Identity & Access Management'' THEN ''IAM''
            WHEN skill_name = ''Java'' THEN ''JAVA''
            WHEN skill_name = ''JavaScript'' THEN ''JAVASCRIPT''
            WHEN skill_name = ''Kubernetes'' THEN ''KUBERNETES''
            WHEN skill_name = ''MuleSoft'' THEN ''MULESOFT''
            WHEN skill_name = ''Node.js'' THEN ''NODEJS''
            WHEN skill_name = ''Performance Testing'' THEN ''PERF_TESTING''
            WHEN skill_name = ''Power BI'' THEN ''POWER_BI''
            WHEN skill_name = ''Python'' THEN ''PYTHON''
            WHEN skill_name = ''React'' THEN ''REACT''
            WHEN skill_name = ''SAP'' THEN ''SAP''
            WHEN skill_name = ''SQL'' THEN ''SQL''
            WHEN skill_name = ''Selenium'' THEN ''SELENIUM''
            WHEN skill_name = ''Snowflake'' THEN ''SNOWFLAKE''
            WHEN skill_name = ''Tableau'' THEN ''TABLEAU''
            WHEN skill_name = ''Terraform'' THEN ''TERRAFORM''
            WHEN skill_name = ''TypeScript'' THEN ''TYPESCRIPT''
            WHEN skill_name = ''dbt'' THEN ''DBT''
            WHEN skill_name = ''iOS'' THEN ''IOS''
            ELSE UPPER(REGEXP_REPLACE(TRIM(skill_name), ''[^a-zA-Z0-9]+'', ''_''))
        END,
           __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS,
           CASE WHEN skill_name IS NULL OR TRIM(skill_name) = '''' THEN ''NULL_REQUIRED_FIELD:skill_name''
                ELSE NULL END AS rejection_reason
    FROM HR_ANALYTICS.BRONZE.SKILLS_STRM
    WHERE METADATA$ACTION = ''INSERT'';

    INSERT INTO HR_ANALYTICS.SILVER.SKILLS
        (skill_id, skill_name, skill_category, added_date, updated_date, is_active, skill_code,
         __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT skill_id, skill_name, skill_category,
           TRY_TO_DATE(added_date), TRY_TO_DATE(updated_date), TRY_TO_BOOLEAN(is_active), skill_code,
           __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, CURRENT_TIMESTAMP()
    FROM TMP_SKILLS_STREAM
    WHERE rejection_reason IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
        (source_layer, source_table, target_table, natural_key_value, src_delta_date, src_file_name, rejection_reason, raw_row_variant, quarantined_at)
    SELECT ''SILVER'', ''HR_ANALYTICS.BRONZE.SKILLS'', ''HR_ANALYTICS.SILVER.SKILLS'',
           TO_VARCHAR(skill_id), NULL, __STG_FILE_NAME, rejection_reason,
           OBJECT_CONSTRUCT(''skill_id'', skill_id, ''skill_name'', skill_name, ''skill_category'', skill_category),
           CURRENT_TIMESTAMP()
    FROM TMP_SKILLS_STREAM
    WHERE rejection_reason IS NOT NULL;
    v_rows_quarantined := SQLROWCOUNT;

    COMMIT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_SKILLS'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_SKILLS succeeded.'');
    RETURN ''SP_LOAD_SILVER_SKILLS: inserted '' || v_rows_inserted || '' row(s), quarantined '' || v_rows_quarantined || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_SKILLS'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_SKILLS failed: '' || v_err_msg);
        RAISE;
END;
';

-- -----------------------------------------------------------------------------
-- 8. SP_LOAD_SILVER_EMPLOYEE_SKILLS -- adds proficiency_rank
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_ANALYTICS.UTIL.SP_LOAD_SILVER_EMPLOYEE_SKILLS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.EMPLOYEE_SKILLS rows (via stream) into SILVER.EMPLOYEE_SKILLS with proper typing, delta lineage, and a proficiency_rank (Beginner=1..Expert=4) derived from proficiency_level; quarantines rows with a blank skill_id or an unresolvable employee_id/skill_id. Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_quarantined INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_EMPLOYEE_SKILLS'', ''SILVER'', ''HR_ANALYTICS.SILVER.EMPLOYEE_SKILLS'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_EMPLOYEE_SKILLS started.'');

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_EMPLOYEE_SKILLS_STREAM (
        employee_skill_id NUMBER,
        employee_id NUMBER,
        skill_id NUMBER,
        proficiency_level VARCHAR(50),
        is_primary_skill VARCHAR(20),
        proficiency_rank NUMBER(1,0),
        __SRC_DELTA_DATE VARCHAR(50),
        __SRC_OPERATION_TYPE VARCHAR(20),
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ,
        rejection_reason VARCHAR(500)
    );
    DELETE FROM TMP_EMPLOYEE_SKILLS_STREAM;

    BEGIN TRANSACTION;

    INSERT INTO TMP_EMPLOYEE_SKILLS_STREAM (employee_skill_id, employee_id, skill_id, proficiency_level, is_primary_skill, proficiency_rank, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, rejection_reason)
        SELECT employee_skill_id, employee_id, skill_id, proficiency_level, is_primary_skill,
        HR_ANALYTICS.UTIL.FN_PROFICIENCY_RANK(proficiency_level),
           __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS,
           CASE WHEN employee_id IS NULL THEN ''NULL_REQUIRED_FIELD:employee_id''
                WHEN skill_id IS NULL THEN ''NULL_REQUIRED_FIELD:skill_id''
                WHEN employee_id NOT IN (SELECT employee_id FROM HR_ANALYTICS.SILVER.EMPLOYEES) THEN ''FK_NOT_FOUND:employee_id''
                WHEN skill_id NOT IN (SELECT skill_id FROM HR_ANALYTICS.SILVER.SKILLS) THEN ''FK_NOT_FOUND:skill_id''
                ELSE NULL END AS rejection_reason
    FROM HR_ANALYTICS.BRONZE.EMPLOYEE_SKILLS_STRM
    WHERE METADATA$ACTION = ''INSERT'';

    INSERT INTO HR_ANALYTICS.SILVER.EMPLOYEE_SKILLS
        (employee_skill_id, employee_id, skill_id, proficiency_level, is_primary_skill, proficiency_rank,
         __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT employee_skill_id, employee_id, skill_id, proficiency_level, TRY_TO_BOOLEAN(is_primary_skill), proficiency_rank,
           TRY_TO_DATE(__SRC_DELTA_DATE), __SRC_OPERATION_TYPE,
           __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, CURRENT_TIMESTAMP()
    FROM TMP_EMPLOYEE_SKILLS_STREAM
    WHERE rejection_reason IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
        (source_layer, source_table, target_table, natural_key_value, src_delta_date, src_file_name, rejection_reason, raw_row_variant, quarantined_at)
    SELECT ''SILVER'', ''HR_ANALYTICS.BRONZE.EMPLOYEE_SKILLS'', ''HR_ANALYTICS.SILVER.EMPLOYEE_SKILLS'',
           TO_VARCHAR(employee_skill_id), TRY_TO_DATE(__SRC_DELTA_DATE), __STG_FILE_NAME, rejection_reason,
           OBJECT_CONSTRUCT(''employee_skill_id'', employee_skill_id, ''employee_id'', employee_id, ''skill_id'', skill_id,
                             ''delta_date'', __SRC_DELTA_DATE, ''operation_type'', __SRC_OPERATION_TYPE),
           CURRENT_TIMESTAMP()
    FROM TMP_EMPLOYEE_SKILLS_STREAM
    WHERE rejection_reason IS NOT NULL;
    v_rows_quarantined := SQLROWCOUNT;

    COMMIT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_EMPLOYEE_SKILLS'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_EMPLOYEE_SKILLS succeeded.'');
    RETURN ''SP_LOAD_SILVER_EMPLOYEE_SKILLS: inserted '' || v_rows_inserted || '' row(s), quarantined '' || v_rows_quarantined || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_EMPLOYEE_SKILLS'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_EMPLOYEE_SKILLS failed: '' || v_err_msg);
        RAISE;
END;
';

-- -----------------------------------------------------------------------------
-- 9. SP_LOAD_SILVER_PROJECT_TECHNOLOGIES -- adds required_proficiency_rank
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_ANALYTICS.UTIL.SP_LOAD_SILVER_PROJECT_TECHNOLOGIES()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.PROJECT_TECHNOLOGIES rows (via stream) into SILVER.PROJECT_TECHNOLOGIES with proper typing, delta lineage, and a required_proficiency_rank (Beginner=1..Expert=4) derived from required_proficiency_level; quarantines rows with an unresolvable project_id/skill_id. Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_quarantined INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_PROJECT_TECHNOLOGIES'', ''SILVER'', ''HR_ANALYTICS.SILVER.PROJECT_TECHNOLOGIES'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_PROJECT_TECHNOLOGIES started.'');

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_PROJECT_TECH_STREAM (
        project_technology_id NUMBER,
        project_id NUMBER,
        skill_id NUMBER,
        required_proficiency_level VARCHAR(50),
        is_primary_technology VARCHAR(20),
        required_proficiency_rank NUMBER(1,0),
        __SRC_DELTA_DATE VARCHAR(50),
        __SRC_OPERATION_TYPE VARCHAR(20),
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ,
        rejection_reason VARCHAR(500)
    );
    DELETE FROM TMP_PROJECT_TECH_STREAM;

    BEGIN TRANSACTION;

    INSERT INTO TMP_PROJECT_TECH_STREAM (project_technology_id, project_id, skill_id, required_proficiency_level, is_primary_technology, required_proficiency_rank, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, rejection_reason)
        SELECT project_technology_id, project_id, skill_id, required_proficiency_level, is_primary_technology,
        HR_ANALYTICS.UTIL.FN_PROFICIENCY_RANK(required_proficiency_level),
           __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS,
           CASE WHEN project_id IS NULL THEN ''NULL_REQUIRED_FIELD:project_id''
                WHEN skill_id IS NULL THEN ''NULL_REQUIRED_FIELD:skill_id''
                WHEN project_id NOT IN (SELECT project_id FROM HR_ANALYTICS.SILVER.PROJECTS) THEN ''FK_NOT_FOUND:project_id''
                WHEN skill_id NOT IN (SELECT skill_id FROM HR_ANALYTICS.SILVER.SKILLS) THEN ''FK_NOT_FOUND:skill_id''
                ELSE NULL END AS rejection_reason
    FROM HR_ANALYTICS.BRONZE.PROJECT_TECHNOLOGIES_STRM
    WHERE METADATA$ACTION = ''INSERT'';

    INSERT INTO HR_ANALYTICS.SILVER.PROJECT_TECHNOLOGIES
        (project_technology_id, project_id, skill_id, required_proficiency_level, is_primary_technology, required_proficiency_rank,
         __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT project_technology_id, project_id, skill_id, required_proficiency_level, TRY_TO_BOOLEAN(is_primary_technology), required_proficiency_rank,
           TRY_TO_DATE(__SRC_DELTA_DATE), __SRC_OPERATION_TYPE,
           __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, CURRENT_TIMESTAMP()
    FROM TMP_PROJECT_TECH_STREAM
    WHERE rejection_reason IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
        (source_layer, source_table, target_table, natural_key_value, src_delta_date, src_file_name, rejection_reason, raw_row_variant, quarantined_at)
    SELECT ''SILVER'', ''HR_ANALYTICS.BRONZE.PROJECT_TECHNOLOGIES'', ''HR_ANALYTICS.SILVER.PROJECT_TECHNOLOGIES'',
           TO_VARCHAR(project_technology_id), TRY_TO_DATE(__SRC_DELTA_DATE), __STG_FILE_NAME, rejection_reason,
           OBJECT_CONSTRUCT(''project_technology_id'', project_technology_id, ''project_id'', project_id, ''skill_id'', skill_id,
                             ''delta_date'', __SRC_DELTA_DATE, ''operation_type'', __SRC_OPERATION_TYPE),
           CURRENT_TIMESTAMP()
    FROM TMP_PROJECT_TECH_STREAM
    WHERE rejection_reason IS NOT NULL;
    v_rows_quarantined := SQLROWCOUNT;

    COMMIT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_PROJECT_TECHNOLOGIES'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_PROJECT_TECHNOLOGIES succeeded.'');
    RETURN ''SP_LOAD_SILVER_PROJECT_TECHNOLOGIES: inserted '' || v_rows_inserted || '' row(s), quarantined '' || v_rows_quarantined || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_PROJECT_TECHNOLOGIES'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_PROJECT_TECHNOLOGIES failed: '' || v_err_msg);
        RAISE;
END;
';

-- -----------------------------------------------------------------------------
-- 10. SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS -- adds access conformance derivations
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_ANALYTICS.UTIL.SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Loads new BRONZE.EMPLOYEE_DAILY_ACCESS rows (via stream) into SILVER.EMPLOYEE_DAILY_ACCESS with proper typing, delta lineage, and access_date_standardized/access_time/access_hour/access_event_type_standardized derivations; quarantines rows with a blank access_id, an access_id that does not resolve to any employee, an unresolvable office_id, an access_event_type other than IN/OUT, an unparsable access_date/access_timestamp, or a access_date/access_timestamp mismatch. Insert-only, stream-deduplicated.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_quarantined INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS'', ''SILVER'', ''HR_ANALYTICS.SILVER.EMPLOYEE_DAILY_ACCESS'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS started.'');

    CREATE TEMPORARY TABLE IF NOT EXISTS TMP_ACCESS_STREAM (
        access_event_id NUMBER,
        access_id VARCHAR(30),
        office_id NUMBER,
        access_date VARCHAR(50),
        access_timestamp VARCHAR(50),
        access_event_type VARCHAR(20),
        office_city VARCHAR(200),
        access_date_standardized DATE,
        access_time TIME,
        access_hour NUMBER(2,0),
        access_event_type_standardized VARCHAR(20),
        is_date_consistent BOOLEAN,
        __SRC_DELTA_DATE VARCHAR(50),
        __SRC_OPERATION_TYPE VARCHAR(20),
        __STG_FILE_NAME VARCHAR(500),
        __STG_FILE_ROW_NUMBER NUMBER,
        __STG_LOAD_TS TIMESTAMP_NTZ,
        rejection_reason VARCHAR(500)
    );
    DELETE FROM TMP_ACCESS_STREAM;

    BEGIN TRANSACTION;

    INSERT INTO TMP_ACCESS_STREAM (access_event_id, access_id, office_id, access_date, access_timestamp, access_event_type, office_city, access_date_standardized, access_time, access_hour, access_event_type_standardized, is_date_consistent, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, rejection_reason)
        SELECT access_event_id, access_id, office_id, access_date, access_timestamp, UPPER(TRIM(access_event_type)), office_city,
           TRY_TO_DATE(access_date) AS access_date_standardized,
           TO_TIME(TRY_TO_TIMESTAMP_NTZ(access_timestamp)) AS access_time,
           HOUR(TRY_TO_TIMESTAMP_NTZ(access_timestamp)) AS access_hour,
           UPPER(TRIM(access_event_type)) AS access_event_type_standardized,
           CASE WHEN TRY_TO_TIMESTAMP_NTZ(access_timestamp) IS NULL OR TRY_TO_DATE(access_date) IS NULL THEN NULL
                ELSE (TO_DATE(TRY_TO_TIMESTAMP_NTZ(access_timestamp)) = TRY_TO_DATE(access_date)) END AS is_date_consistent,
           __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS,
           CASE WHEN access_id IS NULL OR TRIM(access_id) = '''' THEN ''NULL_REQUIRED_FIELD:access_id''
                WHEN access_id NOT IN (SELECT access_id FROM HR_ANALYTICS.SILVER.EMPLOYEES) THEN ''FK_NOT_FOUND:access_id''
                WHEN office_id IS NOT NULL AND office_id NOT IN (SELECT office_id FROM HR_ANALYTICS.SILVER.OFFICES) THEN ''FK_NOT_FOUND:office_id''
                WHEN UPPER(TRIM(access_event_type)) NOT IN (''IN'', ''OUT'') THEN ''INVALID_VALUE:access_event_type''
                WHEN TRY_TO_TIMESTAMP_NTZ(access_timestamp) IS NULL THEN ''INVALID_VALUE:access_timestamp''
                WHEN TRY_TO_DATE(access_date) IS NULL THEN ''INVALID_VALUE:access_date''
                WHEN TO_DATE(TRY_TO_TIMESTAMP_NTZ(access_timestamp)) != TRY_TO_DATE(access_date) THEN ''DATE_TIMESTAMP_MISMATCH:access_date''
                ELSE NULL END AS rejection_reason
    FROM HR_ANALYTICS.BRONZE.EMPLOYEE_DAILY_ACCESS_STRM
    WHERE METADATA$ACTION = ''INSERT'';

    INSERT INTO HR_ANALYTICS.SILVER.EMPLOYEE_DAILY_ACCESS
        (access_event_id, access_id, office_id, access_date, access_timestamp, access_event_type, office_city,
         access_date_standardized, access_time, access_hour, access_event_type_standardized, is_date_consistent,
         __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, __SILVER_LOAD_TS)
    SELECT access_event_id, access_id, office_id,
           TRY_TO_DATE(access_date), TRY_TO_TIMESTAMP_NTZ(access_timestamp), access_event_type, office_city,
           access_date_standardized, access_time, access_hour, access_event_type_standardized, is_date_consistent,
           TRY_TO_DATE(__SRC_DELTA_DATE), __SRC_OPERATION_TYPE,
           __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS, CURRENT_TIMESTAMP()
    FROM TMP_ACCESS_STREAM
    WHERE rejection_reason IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
        (source_layer, source_table, target_table, natural_key_value, src_delta_date, src_file_name, rejection_reason, raw_row_variant, quarantined_at)
    SELECT ''SILVER'', ''HR_ANALYTICS.BRONZE.EMPLOYEE_DAILY_ACCESS'', ''HR_ANALYTICS.SILVER.EMPLOYEE_DAILY_ACCESS'',
           TO_VARCHAR(access_event_id), TRY_TO_DATE(__SRC_DELTA_DATE), __STG_FILE_NAME, rejection_reason,
           OBJECT_CONSTRUCT(''access_event_id'', access_event_id, ''access_id'', access_id, ''office_id'', office_id,
                             ''access_event_type'', access_event_type, ''delta_date'', __SRC_DELTA_DATE, ''operation_type'', __SRC_OPERATION_TYPE),
           CURRENT_TIMESTAMP()
    FROM TMP_ACCESS_STREAM
    WHERE rejection_reason IS NOT NULL;
    v_rows_quarantined := SQLROWCOUNT;

    COMMIT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS succeeded.'');
    RETURN ''SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS: inserted '' || v_rows_inserted || '' row(s), quarantined '' || v_rows_quarantined || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS failed: '' || v_err_msg);
        RAISE;
END;
';

SELECT 'V035 migration applied.' AS status;

SELECT 'V035 migration applied.' AS status;



-- ============================================================================
-- SECTION: V008 - Gold Load Procedures (13 dimension/fact/bridge loaders + orchestration)
-- ============================================================================

-- V008__gold_load_procedures.sql
-- Gold loaders keep SCD2 change detection local to each grain; the coordinator calls them in dependency order.
-- SCD2 loader steps (dimensions, effective-dated facts, and bridges):
--   1. Select the newest Silver row for each business key and calculate its
--      effective date and attribute hash.
--   2. Close the current Gold version only when that hash changed.
--   3. Insert the missing current version, retaining the prior version as
--      history. Type-0/Type-1 reference dimensions use their stated seed or
--      merge pattern instead. The final coordinators call dependencies first.
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_DIM_DEPARTMENT()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='SCD Type-2 load of GOLD.DIM_DEPARTMENT from SILVER.DEPARTMENTS (latest version per department_id). No delta file exists for this entity; every version is dated at the fixed bootstrap epoch.'
EXECUTE AS OWNER
AS '
DECLARE
    v_epoch DATE DEFAULT ''2000-01-01''::DATE;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_closed INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_DIM_DEPARTMENT'', ''GOLD'', ''HR_ANALYTICS.GOLD.DIM_DEPARTMENT'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_DEPARTMENT started.'');

    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_DEPARTMENT AS
    SELECT department_id, department_code, department_name, is_active,
           SHA2(department_id::VARCHAR, 256) AS department_hk,
           SHA2(CONCAT_WS(''||'', department_code, department_name, is_active::VARCHAR), 256) AS row_hash
    FROM HR_ANALYTICS.SILVER.DEPARTMENTS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY __SILVER_LOAD_TS DESC) = 1;

    UPDATE HR_ANALYTICS.GOLD.DIM_DEPARTMENT d
       SET __EFFECTIVE_TO_DATE = DATEADD(''day'', -1, :v_epoch), __IS_CURRENT = FALSE
     WHERE d.__IS_CURRENT = TRUE
       AND EXISTS (SELECT 1 FROM TMP_LATEST_DEPARTMENT s WHERE s.department_id = d.department_id AND s.row_hash <> d.__ROW_HASH);
    v_rows_closed := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOLD.DIM_DEPARTMENT
        (department_hk, department_id, department_code, department_name, is_active,
         __EFFECTIVE_FROM_DATE, __EFFECTIVE_TO_DATE, __IS_CURRENT, __ROW_HASH, __GOLD_LOAD_TS)
    SELECT s.department_hk, s.department_id, s.department_code, s.department_name, s.is_active,
           :v_epoch, ''9999-12-31''::DATE, TRUE, s.row_hash, CURRENT_TIMESTAMP()
    FROM TMP_LATEST_DEPARTMENT s
    LEFT JOIN HR_ANALYTICS.GOLD.DIM_DEPARTMENT d ON d.department_id = s.department_id AND d.__IS_CURRENT = TRUE
    WHERE d.department_id IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_DIM_DEPARTMENT'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_DEPARTMENT succeeded.'');
    RETURN ''SP_LOAD_GOLD_DIM_DEPARTMENT: closed '' || v_rows_closed || '' row(s), inserted '' || v_rows_inserted || '' new version(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_DIM_DEPARTMENT'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_DIM_DEPARTMENT failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_DIM_OFFICE()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='SCD Type-2 load of GOLD.DIM_OFFICE from SILVER.OFFICES (latest version per office_id). No delta file exists for this entity; every version is dated at the fixed bootstrap epoch.'
EXECUTE AS OWNER
AS '
DECLARE
    v_epoch DATE DEFAULT ''2000-01-01''::DATE;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_closed INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_DIM_OFFICE'', ''GOLD'', ''HR_ANALYTICS.GOLD.DIM_OFFICE'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_OFFICE started.'');

    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_OFFICE AS
    SELECT office_id, office_code, office_city, office_country, office_region, is_active,
           SHA2(office_id::VARCHAR, 256) AS office_hk,
           SHA2(CONCAT_WS(''||'', office_code, office_city, office_country, office_region, is_active::VARCHAR), 256) AS row_hash
    FROM HR_ANALYTICS.SILVER.OFFICES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY office_id ORDER BY __SILVER_LOAD_TS DESC) = 1;

    UPDATE HR_ANALYTICS.GOLD.DIM_OFFICE d
       SET __EFFECTIVE_TO_DATE = DATEADD(''day'', -1, :v_epoch), __IS_CURRENT = FALSE
     WHERE d.__IS_CURRENT = TRUE
       AND EXISTS (SELECT 1 FROM TMP_LATEST_OFFICE s WHERE s.office_id = d.office_id AND s.row_hash <> d.__ROW_HASH);
    v_rows_closed := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOLD.DIM_OFFICE
        (office_hk, office_id, office_code, office_city, office_country, office_region, is_active,
         __EFFECTIVE_FROM_DATE, __EFFECTIVE_TO_DATE, __IS_CURRENT, __ROW_HASH, __GOLD_LOAD_TS)
    SELECT s.office_hk, s.office_id, s.office_code, s.office_city, s.office_country, s.office_region, s.is_active,
           :v_epoch, ''9999-12-31''::DATE, TRUE, s.row_hash, CURRENT_TIMESTAMP()
    FROM TMP_LATEST_OFFICE s
    LEFT JOIN HR_ANALYTICS.GOLD.DIM_OFFICE d ON d.office_id = s.office_id AND d.__IS_CURRENT = TRUE
    WHERE d.office_id IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_DIM_OFFICE'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_OFFICE succeeded.'');
    RETURN ''SP_LOAD_GOLD_DIM_OFFICE: closed '' || v_rows_closed || '' row(s), inserted '' || v_rows_inserted || '' new version(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_DIM_OFFICE'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_DIM_OFFICE failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_DIM_COMPANY()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='SCD Type-2 load of GOLD.DIM_COMPANY from SILVER.COMPANIES (latest version per company_id). Effective date is the delta file delta_date when present, else the bootstrap epoch; the prior version is expired the day before the new effective date.'
EXECUTE AS OWNER
AS '
DECLARE
    v_epoch DATE DEFAULT ''2000-01-01''::DATE;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_closed INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_DIM_COMPANY'', ''GOLD'', ''HR_ANALYTICS.GOLD.DIM_COMPANY'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_COMPANY started.'');

    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_COMPANY AS
    SELECT company_id, company_name, industry, company_country, company_classification, is_active,
           COALESCE(__SRC_DELTA_DATE, :v_epoch) AS effective_from_date,
           SHA2(company_id::VARCHAR, 256) AS company_hk,
           SHA2(CONCAT_WS(''||'', company_name, industry, company_country, company_classification, is_active::VARCHAR), 256) AS row_hash
    FROM HR_ANALYTICS.SILVER.COMPANIES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY __SILVER_LOAD_TS DESC) = 1;

    UPDATE HR_ANALYTICS.GOLD.DIM_COMPANY d
       SET __EFFECTIVE_TO_DATE = DATEADD(''day'', -1, s.effective_from_date), __IS_CURRENT = FALSE
      FROM TMP_LATEST_COMPANY s
     WHERE d.company_id = s.company_id AND d.__IS_CURRENT = TRUE AND s.row_hash <> d.__ROW_HASH;
    v_rows_closed := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOLD.DIM_COMPANY
        (company_hk, company_id, company_name, industry, company_country, company_classification, is_active,
         __EFFECTIVE_FROM_DATE, __EFFECTIVE_TO_DATE, __IS_CURRENT, __ROW_HASH, __GOLD_LOAD_TS)
    SELECT s.company_hk, s.company_id, s.company_name, s.industry, s.company_country, s.company_classification, s.is_active,
           s.effective_from_date, ''9999-12-31''::DATE, TRUE, s.row_hash, CURRENT_TIMESTAMP()
    FROM TMP_LATEST_COMPANY s
    LEFT JOIN HR_ANALYTICS.GOLD.DIM_COMPANY d ON d.company_id = s.company_id AND d.__IS_CURRENT = TRUE
    WHERE d.company_id IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_DIM_COMPANY'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_COMPANY succeeded.'');
    RETURN ''SP_LOAD_GOLD_DIM_COMPANY: closed '' || v_rows_closed || '' row(s), inserted '' || v_rows_inserted || '' new version(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_DIM_COMPANY'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_DIM_COMPANY failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_DIM_EMPLOYEE()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='SCD Type-2 load of GOLD.DIM_EMPLOYEE from SILVER.EMPLOYEES (latest version per employee_id). Effective date is the delta file delta_date when present, else the bootstrap epoch.'
EXECUTE AS OWNER
AS '
DECLARE
    v_epoch DATE DEFAULT ''2000-01-01''::DATE;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_closed INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_DIM_EMPLOYEE'', ''GOLD'', ''HR_ANALYTICS.GOLD.DIM_EMPLOYEE'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_EMPLOYEE started.'');

    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_EMPLOYEE AS
    SELECT
        employee_id, access_id, employee_name, employee_email, department_id, office_id, manager_employee_id,
        job_title, job_level, employment_status, hire_date, is_active,
        COALESCE(__SRC_DELTA_DATE, :v_epoch) AS effective_from_date,
        SHA2(employee_id::VARCHAR, 256) AS employee_hk,
        SHA2(department_id::VARCHAR, 256) AS department_hk,
        SHA2(office_id::VARCHAR, 256) AS office_hk,
        CASE WHEN manager_employee_id IS NULL THEN NULL ELSE SHA2(manager_employee_id::VARCHAR, 256) END AS manager_employee_hk,
        SHA2(CONCAT_WS(''||'', access_id, employee_name, employee_email, department_id::VARCHAR, office_id::VARCHAR,
                        COALESCE(manager_employee_id::VARCHAR,''NA''), job_title, job_level, employment_status,
                        hire_date::VARCHAR, is_active::VARCHAR), 256) AS row_hash
    FROM HR_ANALYTICS.SILVER.EMPLOYEES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY __SILVER_LOAD_TS DESC) = 1;

    UPDATE HR_ANALYTICS.GOLD.DIM_EMPLOYEE d
       SET __EFFECTIVE_TO_DATE = DATEADD(''day'', -1, s.effective_from_date), __IS_CURRENT = FALSE
      FROM TMP_LATEST_EMPLOYEE s
     WHERE d.employee_id = s.employee_id AND d.__IS_CURRENT = TRUE AND s.row_hash <> d.__ROW_HASH;
    v_rows_closed := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOLD.DIM_EMPLOYEE
        (employee_hk, employee_id, access_id, employee_name, employee_email, department_hk, office_hk, manager_employee_hk,
         job_title, job_level, employment_status, hire_date, is_active,
         __EFFECTIVE_FROM_DATE, __EFFECTIVE_TO_DATE, __IS_CURRENT, __ROW_HASH, __GOLD_LOAD_TS)
    SELECT s.employee_hk, s.employee_id, s.access_id, s.employee_name, s.employee_email, s.department_hk, s.office_hk, s.manager_employee_hk,
           s.job_title, s.job_level, s.employment_status, s.hire_date, s.is_active,
           s.effective_from_date, ''9999-12-31''::DATE, TRUE, s.row_hash, CURRENT_TIMESTAMP()
    FROM TMP_LATEST_EMPLOYEE s
    LEFT JOIN HR_ANALYTICS.GOLD.DIM_EMPLOYEE d ON d.employee_id = s.employee_id AND d.__IS_CURRENT = TRUE
    WHERE d.employee_id IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_DIM_EMPLOYEE'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_EMPLOYEE succeeded.'');
    RETURN ''SP_LOAD_GOLD_DIM_EMPLOYEE: closed '' || v_rows_closed || '' row(s), inserted '' || v_rows_inserted || '' new version(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_DIM_EMPLOYEE'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_DIM_EMPLOYEE failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_DIM_PROJECT()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='SCD Type-2 load of GOLD.DIM_PROJECT from SILVER.PROJECTS (latest version per project_id), excluding project_budget_usd which is now modeled as FCT_PROJECT_BUDGET_PLAN. Effective date is the delta file delta_date when present, else the bootstrap epoch.'
EXECUTE AS OWNER
AS '
DECLARE
    v_epoch DATE DEFAULT ''2000-01-01''::DATE;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_closed INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_DIM_PROJECT'', ''GOLD'', ''HR_ANALYTICS.GOLD.DIM_PROJECT'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_PROJECT started.'');

    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_PROJECT AS
    SELECT
        project_id, project_name, company_id, owning_department_id, project_type, project_billing_type,
        project_status, start_date, planned_end_date, actual_end_date, is_active,
        COALESCE(__SRC_DELTA_DATE, :v_epoch) AS effective_from_date,
        SHA2(project_id::VARCHAR, 256) AS project_hk,
        SHA2(company_id::VARCHAR, 256) AS company_hk,
        SHA2(owning_department_id::VARCHAR, 256) AS owning_department_hk,
        SHA2(CONCAT_WS(''||'', project_name, company_id::VARCHAR, owning_department_id::VARCHAR, project_type, project_billing_type,
                        project_status, start_date::VARCHAR, planned_end_date::VARCHAR,
                        COALESCE(actual_end_date::VARCHAR,''NA''), is_active::VARCHAR), 256) AS row_hash
    FROM HR_ANALYTICS.SILVER.PROJECTS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY __SILVER_LOAD_TS DESC) = 1;

    UPDATE HR_ANALYTICS.GOLD.DIM_PROJECT d
       SET __EFFECTIVE_TO_DATE = DATEADD(''day'', -1, s.effective_from_date), __IS_CURRENT = FALSE
      FROM TMP_LATEST_PROJECT s
     WHERE d.project_id = s.project_id AND d.__IS_CURRENT = TRUE AND s.row_hash <> d.__ROW_HASH;
    v_rows_closed := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOLD.DIM_PROJECT
        (project_hk, project_id, project_name, company_hk, owning_department_hk, project_type, project_billing_type,
         project_status, start_date, planned_end_date, actual_end_date, is_active,
         __EFFECTIVE_FROM_DATE, __EFFECTIVE_TO_DATE, __IS_CURRENT, __ROW_HASH, __GOLD_LOAD_TS)
    SELECT s.project_hk, s.project_id, s.project_name, s.company_hk, s.owning_department_hk, s.project_type, s.project_billing_type,
           s.project_status, s.start_date, s.planned_end_date, s.actual_end_date, s.is_active,
           s.effective_from_date, ''9999-12-31''::DATE, TRUE, s.row_hash, CURRENT_TIMESTAMP()
    FROM TMP_LATEST_PROJECT s
    LEFT JOIN HR_ANALYTICS.GOLD.DIM_PROJECT d ON d.project_id = s.project_id AND d.__IS_CURRENT = TRUE
    WHERE d.project_id IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_DIM_PROJECT'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_PROJECT succeeded.'');
    RETURN ''SP_LOAD_GOLD_DIM_PROJECT: closed '' || v_rows_closed || '' row(s), inserted '' || v_rows_inserted || '' new version(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_DIM_PROJECT'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_DIM_PROJECT failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_DIM_SKILL()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Type 1 upsert of SILVER.SKILLS into GOLD.DIM_SKILL. Inserts new skills, overwrites changed attributes in place for existing skills (no version history kept).'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_affected INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_DIM_SKILL'', ''GOLD'', ''HR_ANALYTICS.GOLD.DIM_SKILL'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_SKILL started.'');

    MERGE INTO HR_ANALYTICS.GOLD.DIM_SKILL tgt
    USING (
        SELECT skill_id, skill_name, skill_category, is_active,
               SHA2(TO_VARCHAR(skill_id), 256) AS skill_hk,
               SHA2(CONCAT_WS(''|'', skill_name, skill_category, TO_VARCHAR(is_active)), 256) AS row_hash
        FROM HR_ANALYTICS.SILVER.SKILLS
        QUALIFY ROW_NUMBER() OVER (PARTITION BY skill_id ORDER BY __SILVER_LOAD_TS DESC) = 1
    ) src
    ON tgt.skill_hk = src.skill_hk
    WHEN MATCHED AND tgt.__ROW_HASH != src.row_hash THEN UPDATE SET
        skill_name = src.skill_name, skill_category = src.skill_category, is_active = src.is_active,
        __ROW_HASH = src.row_hash, __GOLD_LOAD_TS = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (skill_hk, skill_id, skill_name, skill_category, is_active, __ROW_HASH, __GOLD_LOAD_TS)
    VALUES
        (src.skill_hk, src.skill_id, src.skill_name, src.skill_category, src.is_active, src.row_hash, CURRENT_TIMESTAMP());

    v_rows_affected := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_affected, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_DIM_SKILL'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_SKILL succeeded.'');
    RETURN ''SP_LOAD_GOLD_DIM_SKILL: affected '' || v_rows_affected || '' row(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_DIM_SKILL'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_DIM_SKILL failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_DIM_ASSIGNMENT_ROLE()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Seeds/refreshes GOLD.DIM_ASSIGNMENT_ROLE from the distinct assignment_role values observed in SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_affected INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_DIM_ASSIGNMENT_ROLE'', ''GOLD'', ''HR_ANALYTICS.GOLD.DIM_ASSIGNMENT_ROLE'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_ASSIGNMENT_ROLE started.'');

    MERGE INTO HR_ANALYTICS.GOLD.DIM_ASSIGNMENT_ROLE tgt
    USING (
        SELECT DISTINCT assignment_role, SHA2(assignment_role, 256) AS assignment_role_hk
        FROM HR_ANALYTICS.SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS
        WHERE assignment_role IS NOT NULL
    ) src
    ON tgt.assignment_role_hk = src.assignment_role_hk
    WHEN NOT MATCHED THEN INSERT (assignment_role_hk, assignment_role, __GOLD_LOAD_TS)
    VALUES (src.assignment_role_hk, src.assignment_role, CURRENT_TIMESTAMP());

    v_rows_affected := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_affected, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_DIM_ASSIGNMENT_ROLE'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_ASSIGNMENT_ROLE succeeded.'');
    RETURN ''SP_LOAD_GOLD_DIM_ASSIGNMENT_ROLE: inserted '' || v_rows_affected || '' new row(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_DIM_ASSIGNMENT_ROLE'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_DIM_ASSIGNMENT_ROLE failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_DIM_PROFICIENCY()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Seeds/refreshes GOLD.DIM_PROFICIENCY with the 4 fixed proficiency bands and their ordinal rank.'
EXECUTE AS OWNER
AS '
DECLARE
    v_rows_affected INTEGER DEFAULT 0;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_DIM_PROFICIENCY'', ''GOLD'', ''HR_ANALYTICS.GOLD.DIM_PROFICIENCY'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_PROFICIENCY started.'');

    MERGE INTO HR_ANALYTICS.GOLD.DIM_PROFICIENCY tgt
    USING (
        SELECT column1 AS proficiency_level, column2 AS proficiency_rank, SHA2(column1, 256) AS proficiency_hk
        FROM (VALUES (''Beginner'', 1), (''Intermediate'', 2), (''Advanced'', 3), (''Expert'', 4))
    ) src
    ON tgt.proficiency_hk = src.proficiency_hk
    WHEN NOT MATCHED THEN INSERT (proficiency_hk, proficiency_level, proficiency_rank, __GOLD_LOAD_TS)
    VALUES (src.proficiency_hk, src.proficiency_level, src.proficiency_rank, CURRENT_TIMESTAMP());

    v_rows_affected := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_affected, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_DIM_PROFICIENCY'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_DIM_PROFICIENCY succeeded.'');
    RETURN ''SP_LOAD_GOLD_DIM_PROFICIENCY: inserted '' || v_rows_affected || '' new row(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_DIM_PROFICIENCY'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_DIM_PROFICIENCY failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_FCT_PROJECT_BUDGET_PLAN()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Effective-dated load of GOLD.FCT_PROJECT_BUDGET_PLAN from SILVER.PROJECTS (latest version per project_id). project_budget_usd is never carried on DIM_PROJECT; only this fact tracks its version history.'
EXECUTE AS OWNER
AS '
DECLARE
    v_epoch DATE DEFAULT ''2000-01-01''::DATE;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_closed INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_FCT_PROJECT_BUDGET_PLAN'', ''GOLD'', ''HR_ANALYTICS.GOLD.FCT_PROJECT_BUDGET_PLAN'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_FCT_PROJECT_BUDGET_PLAN started.'');

    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_BUDGET AS
    SELECT
        project_id, company_id, owning_department_id, project_budget_usd,
        COALESCE(__SRC_DELTA_DATE, :v_epoch) AS effective_from_date,
        SHA2(project_id::VARCHAR, 256) AS project_hk,
        SHA2(company_id::VARCHAR, 256) AS company_hk,
        SHA2(owning_department_id::VARCHAR, 256) AS owning_department_hk,
        SHA2(project_budget_usd::VARCHAR, 256) AS row_hash
    FROM HR_ANALYTICS.SILVER.PROJECTS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY __SILVER_LOAD_TS DESC) = 1;

    UPDATE HR_ANALYTICS.GOLD.FCT_PROJECT_BUDGET_PLAN f
       SET __EFFECTIVE_TO_DATE = DATEADD(''day'', -1, s.effective_from_date), __IS_CURRENT = FALSE
      FROM TMP_LATEST_BUDGET s
     WHERE f.project_id = s.project_id AND f.__IS_CURRENT = TRUE AND s.row_hash <> f.__ROW_HASH;
    v_rows_closed := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOLD.FCT_PROJECT_BUDGET_PLAN
        (project_budget_version_hk, project_id, project_hk, company_hk, owning_department_hk, project_budget_usd,
         __EFFECTIVE_FROM_DATE, __EFFECTIVE_TO_DATE, __IS_CURRENT, __ROW_HASH, __GOLD_LOAD_TS)
    SELECT SHA2(s.project_id::VARCHAR || ''|'' || s.effective_from_date::VARCHAR, 256), s.project_id, s.project_hk, s.company_hk, s.owning_department_hk, s.project_budget_usd,
           s.effective_from_date, ''9999-12-31''::DATE, TRUE, s.row_hash, CURRENT_TIMESTAMP()
    FROM TMP_LATEST_BUDGET s
    LEFT JOIN HR_ANALYTICS.GOLD.FCT_PROJECT_BUDGET_PLAN f ON f.project_id = s.project_id AND f.__IS_CURRENT = TRUE
    WHERE f.project_id IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_FCT_PROJECT_BUDGET_PLAN'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_FCT_PROJECT_BUDGET_PLAN succeeded.'');
    RETURN ''SP_LOAD_GOLD_FCT_PROJECT_BUDGET_PLAN: closed '' || v_rows_closed || '' row(s), inserted '' || v_rows_inserted || '' new version(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_FCT_PROJECT_BUDGET_PLAN'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_FCT_PROJECT_BUDGET_PLAN failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_FACT_EMPLOYEE_PROJECT_ASSIGNMENT()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Effective-dated load of GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT from SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS (latest version per assignment_id). Effective date is the delta file delta_date when present, else the bootstrap epoch.'
EXECUTE AS OWNER
AS '
DECLARE
    v_epoch DATE DEFAULT ''2000-01-01''::DATE;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_closed INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_FACT_EMPLOYEE_PROJECT_ASSIGNMENT'', ''GOLD'', ''HR_ANALYTICS.GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_FACT_EMPLOYEE_PROJECT_ASSIGNMENT started.'');

    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_ASSIGNMENT AS
    SELECT
        assignment_id, employee_id, project_id, assignment_role, allocation_percent, assignment_start_date, assignment_end_date,
        COALESCE(__SRC_DELTA_DATE, :v_epoch) AS effective_from_date,
        SHA2(employee_id::VARCHAR, 256) AS employee_hk,
        SHA2(project_id::VARCHAR, 256) AS project_hk,
        SHA2(assignment_role, 256) AS assignment_role_hk,
        SHA2(CONCAT_WS(''||'', employee_id::VARCHAR, project_id::VARCHAR, assignment_role, allocation_percent::VARCHAR,
                        assignment_start_date::VARCHAR, COALESCE(assignment_end_date::VARCHAR,''NA'')), 256) AS row_hash
    FROM HR_ANALYTICS.SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY assignment_id ORDER BY __SILVER_LOAD_TS DESC) = 1;

    UPDATE HR_ANALYTICS.GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT f
       SET __EFFECTIVE_TO_DATE = DATEADD(''day'', -1, s.effective_from_date), __IS_CURRENT = FALSE
      FROM TMP_LATEST_ASSIGNMENT s
     WHERE f.assignment_id = s.assignment_id AND f.__IS_CURRENT = TRUE AND s.row_hash <> f.__ROW_HASH;
    v_rows_closed := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT
        (assignment_version_hk, assignment_id, employee_hk, project_hk, assignment_role_hk, allocation_percent,
         assignment_start_date, assignment_end_date, __EFFECTIVE_FROM_DATE, __EFFECTIVE_TO_DATE, __IS_CURRENT, __ROW_HASH, __GOLD_LOAD_TS)
    SELECT SHA2(s.assignment_id::VARCHAR || ''|'' || s.effective_from_date::VARCHAR, 256), s.assignment_id, s.employee_hk, s.project_hk, s.assignment_role_hk,
           s.allocation_percent, s.assignment_start_date, s.assignment_end_date,
           s.effective_from_date, ''9999-12-31''::DATE, TRUE, s.row_hash, CURRENT_TIMESTAMP()
    FROM TMP_LATEST_ASSIGNMENT s
    LEFT JOIN HR_ANALYTICS.GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT f ON f.assignment_id = s.assignment_id AND f.__IS_CURRENT = TRUE
    WHERE f.assignment_id IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_FACT_EMPLOYEE_PROJECT_ASSIGNMENT'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_FACT_EMPLOYEE_PROJECT_ASSIGNMENT succeeded.'');
    RETURN ''SP_LOAD_GOLD_FACT_EMPLOYEE_PROJECT_ASSIGNMENT: closed '' || v_rows_closed || '' row(s), inserted '' || v_rows_inserted || '' new version(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_FACT_EMPLOYEE_PROJECT_ASSIGNMENT'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_FACT_EMPLOYEE_PROJECT_ASSIGNMENT failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_BR_EMPLOYEE_SKILL()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Effective-dated load of GOLD.BR_EMPLOYEE_SKILL from SILVER.EMPLOYEE_SKILLS (latest version per employee_skill_id). Effective date is the delta file delta_date when present, else the bootstrap epoch.'
EXECUTE AS OWNER
AS '
DECLARE
    v_epoch DATE DEFAULT ''2000-01-01''::DATE;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_closed INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_BR_EMPLOYEE_SKILL'', ''GOLD'', ''HR_ANALYTICS.GOLD.BR_EMPLOYEE_SKILL'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_BR_EMPLOYEE_SKILL started.'');

    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_EMP_SKILL AS
    SELECT
        employee_skill_id, employee_id, skill_id, proficiency_level, is_primary_skill,
        COALESCE(__SRC_DELTA_DATE, :v_epoch) AS effective_from_date,
        SHA2(employee_id::VARCHAR, 256) AS employee_hk,
        SHA2(skill_id::VARCHAR, 256) AS skill_hk,
        SHA2(proficiency_level, 256) AS proficiency_hk,
        SHA2(CONCAT_WS(''||'', employee_id::VARCHAR, skill_id::VARCHAR, proficiency_level, is_primary_skill::VARCHAR), 256) AS row_hash
    FROM HR_ANALYTICS.SILVER.EMPLOYEE_SKILLS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_skill_id ORDER BY __SILVER_LOAD_TS DESC) = 1;

    UPDATE HR_ANALYTICS.GOLD.BR_EMPLOYEE_SKILL b
       SET __EFFECTIVE_TO_DATE = DATEADD(''day'', -1, s.effective_from_date), __IS_CURRENT = FALSE
      FROM TMP_LATEST_EMP_SKILL s
     WHERE b.employee_skill_id = s.employee_skill_id AND b.__IS_CURRENT = TRUE AND s.row_hash <> b.__ROW_HASH;
    v_rows_closed := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOLD.BR_EMPLOYEE_SKILL
        (employee_skill_version_hk, employee_skill_id, employee_hk, skill_hk, proficiency_hk, is_primary_skill,
         __EFFECTIVE_FROM_DATE, __EFFECTIVE_TO_DATE, __IS_CURRENT, __ROW_HASH, __GOLD_LOAD_TS)
    SELECT SHA2(s.employee_skill_id::VARCHAR || ''|'' || s.effective_from_date::VARCHAR, 256), s.employee_skill_id, s.employee_hk, s.skill_hk, s.proficiency_hk,
           s.is_primary_skill, s.effective_from_date, ''9999-12-31''::DATE, TRUE, s.row_hash, CURRENT_TIMESTAMP()
    FROM TMP_LATEST_EMP_SKILL s
    LEFT JOIN HR_ANALYTICS.GOLD.BR_EMPLOYEE_SKILL b ON b.employee_skill_id = s.employee_skill_id AND b.__IS_CURRENT = TRUE
    WHERE b.employee_skill_id IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_BR_EMPLOYEE_SKILL'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_BR_EMPLOYEE_SKILL succeeded.'');
    RETURN ''SP_LOAD_GOLD_BR_EMPLOYEE_SKILL: closed '' || v_rows_closed || '' row(s), inserted '' || v_rows_inserted || '' new version(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_BR_EMPLOYEE_SKILL'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_BR_EMPLOYEE_SKILL failed: '' || v_err_msg);
        RAISE;
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_BR_PROJECT_SKILL_REQUIREMENT()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Effective-dated load of GOLD.BR_PROJECT_SKILL_REQUIREMENT from SILVER.PROJECT_TECHNOLOGIES (latest version per project_technology_id). Effective date is the delta file delta_date when present, else the bootstrap epoch.'
EXECUTE AS OWNER
AS '
DECLARE
    v_epoch DATE DEFAULT ''2000-01-01''::DATE;
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_closed INTEGER DEFAULT 0;
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES (''SP_LOAD_GOLD_BR_PROJECT_SKILL_REQUIREMENT'', ''GOLD'', ''HR_ANALYTICS.GOLD.BR_PROJECT_SKILL_REQUIREMENT'', ''STARTED'', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_BR_PROJECT_SKILL_REQUIREMENT started.'');

    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_PROJ_TECH AS
    SELECT
        project_technology_id, project_id, skill_id, required_proficiency_level, is_primary_technology,
        COALESCE(__SRC_DELTA_DATE, :v_epoch) AS effective_from_date,
        SHA2(project_id::VARCHAR, 256) AS project_hk,
        SHA2(skill_id::VARCHAR, 256) AS skill_hk,
        SHA2(required_proficiency_level, 256) AS proficiency_hk,
        SHA2(CONCAT_WS(''||'', project_id::VARCHAR, skill_id::VARCHAR, required_proficiency_level, is_primary_technology::VARCHAR), 256) AS row_hash
    FROM HR_ANALYTICS.SILVER.PROJECT_TECHNOLOGIES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY project_technology_id ORDER BY __SILVER_LOAD_TS DESC) = 1;

    UPDATE HR_ANALYTICS.GOLD.BR_PROJECT_SKILL_REQUIREMENT b
       SET __EFFECTIVE_TO_DATE = DATEADD(''day'', -1, s.effective_from_date), __IS_CURRENT = FALSE
      FROM TMP_LATEST_PROJ_TECH s
     WHERE b.project_technology_id = s.project_technology_id AND b.__IS_CURRENT = TRUE AND s.row_hash <> b.__ROW_HASH;
    v_rows_closed := SQLROWCOUNT;

    INSERT INTO HR_ANALYTICS.GOLD.BR_PROJECT_SKILL_REQUIREMENT
        (project_technology_version_hk, project_technology_id, project_hk, skill_hk, proficiency_hk, is_primary_technology,
         __EFFECTIVE_FROM_DATE, __EFFECTIVE_TO_DATE, __IS_CURRENT, __ROW_HASH, __GOLD_LOAD_TS)
    SELECT SHA2(s.project_technology_id::VARCHAR || ''|'' || s.effective_from_date::VARCHAR, 256), s.project_technology_id, s.project_hk, s.skill_hk, s.proficiency_hk,
           s.is_primary_technology, s.effective_from_date, ''9999-12-31''::DATE, TRUE, s.row_hash, CURRENT_TIMESTAMP()
    FROM TMP_LATEST_PROJ_TECH s
    LEFT JOIN HR_ANALYTICS.GOLD.BR_PROJECT_SKILL_REQUIREMENT b ON b.project_technology_id = s.project_technology_id AND b.__IS_CURRENT = TRUE
    WHERE b.project_technology_id IS NULL;
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = ''SUCCESS'', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = ''SP_LOAD_GOLD_BR_PROJECT_SKILL_REQUIREMENT'' AND start_ts = :v_start_ts AND status = ''STARTED'';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''INFO'', ''SP_LOAD_GOLD_BR_PROJECT_SKILL_REQUIREMENT succeeded.'');
    RETURN ''SP_LOAD_GOLD_BR_PROJECT_SKILL_REQUIREMENT: closed '' || v_rows_closed || '' row(s), inserted '' || v_rows_inserted || '' new version(s).'';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = ''FAILED'', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF(''millisecond'', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = ''SP_LOAD_GOLD_BR_PROJECT_SKILL_REQUIREMENT'' AND start_ts = :v_start_ts AND status = ''STARTED'';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT(''ERROR'', ''SP_LOAD_GOLD_BR_PROJECT_SKILL_REQUIREMENT failed: '' || v_err_msg);
        RAISE;
END;
';
-- Access facts resolve against the current employee dimension. This avoids fan-out from Silver history.
CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_FACT_EMPLOYEE_DAILY_ACCESS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Incremental insert-only load of GOLD.FACT_EMPLOYEE_DAILY_ACCESS from SILVER.EMPLOYEE_DAILY_ACCESS, resolving employee_hk via access_id against the single CURRENT GOLD.DIM_EMPLOYEE version (not raw SILVER.EMPLOYEES, which retains every historical row per access_id and previously fanned out duplicate access-event rows - fixed per etl-sp-stream-task-observation.md finding #2/#3). Only access_event_id business keys not yet present in the fact are loaded.'
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_inserted INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES ('SP_LOAD_GOLD_FACT_EMPLOYEE_DAILY_ACCESS', 'GOLD', 'HR_ANALYTICS.GOLD.FACT_EMPLOYEE_DAILY_ACCESS', 'STARTED', :v_start_ts);

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT('INFO', 'SP_LOAD_GOLD_FACT_EMPLOYEE_DAILY_ACCESS started.');

    INSERT INTO HR_ANALYTICS.GOLD.FACT_EMPLOYEE_DAILY_ACCESS
        (access_event_hk, access_event_id, employee_hk, office_hk, access_date, access_timestamp, access_event_type, __GOLD_LOAD_TS)
    SELECT
        SHA2(s.access_event_id::VARCHAR, 256), s.access_event_id,
        d.employee_hk,
        SHA2(s.office_id::VARCHAR, 256),
        s.access_date, s.access_timestamp, s.access_event_type,
        CURRENT_TIMESTAMP()
    FROM HR_ANALYTICS.SILVER.EMPLOYEE_DAILY_ACCESS s
    LEFT JOIN HR_ANALYTICS.GOLD.DIM_EMPLOYEE d ON d.access_id = s.access_id AND d.__IS_CURRENT = TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM HR_ANALYTICS.GOLD.FACT_EMPLOYEE_DAILY_ACCESS f
         WHERE f.access_event_id = s.access_event_id
    );
    v_rows_inserted := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = 'SUCCESS', rows_processed = :v_rows_inserted, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = 'SP_LOAD_GOLD_FACT_EMPLOYEE_DAILY_ACCESS' AND start_ts = :v_start_ts AND status = 'STARTED';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT('INFO', 'SP_LOAD_GOLD_FACT_EMPLOYEE_DAILY_ACCESS succeeded.');
    RETURN 'SP_LOAD_GOLD_FACT_EMPLOYEE_DAILY_ACCESS: inserted ' || v_rows_inserted || ' row(s).';
END;
$$
;

-- SP_LOAD_GOLD_FCT_EMPLOYEE_DAILY_ATTENDANCE steps:
-- 1. Order immutable event facts by employee/date/timestamp.
-- 2. Treat only an IN immediately followed by OUT as a payable pair.
-- 3. Sum valid pair durations and expose unmatched events instead of guessing.
-- 4. MERGE the derived daily result, so a late OUT revises only the daily view
--    of attendance while the underlying event fact remains unchanged.
CREATE OR REPLACE PROCEDURE UTIL.SP_LOAD_GOLD_FCT_EMPLOYEE_DAILY_ATTENDANCE()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Recomputes one Gold attendance row per employee/date from ordered badge events. worked_minutes is the sum of valid adjacent IN-to-OUT pairs; unmatched IN/OUT events are counted and make is_complete_day false.'
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_rows_merged INTEGER DEFAULT 0;
    v_err_msg VARCHAR;
BEGIN
    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES ('SP_LOAD_GOLD_FCT_EMPLOYEE_DAILY_ATTENDANCE', 'GOLD', 'HR_ANALYTICS.GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE', 'STARTED', :v_start_ts);

    MERGE INTO HR_ANALYTICS.GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE t
    USING (
        WITH ordered_events AS (
            SELECT employee_hk, access_date, access_event_id, access_timestamp, access_event_type,
                   LEAD(access_event_type) OVER (PARTITION BY employee_hk, access_date ORDER BY access_timestamp, access_event_id) AS next_event_type,
                   LEAD(access_timestamp) OVER (PARTITION BY employee_hk, access_date ORDER BY access_timestamp, access_event_id) AS next_event_timestamp,
                   LAG(access_event_type) OVER (PARTITION BY employee_hk, access_date ORDER BY access_timestamp, access_event_id) AS previous_event_type
            FROM HR_ANALYTICS.GOLD.FACT_EMPLOYEE_DAILY_ACCESS
            WHERE employee_hk IS NOT NULL
        ), daily_rollup AS (
            SELECT employee_hk,
                   access_date,
                   MIN(IFF(access_event_type = 'IN', access_timestamp, NULL)) AS first_in_timestamp,
                   MAX(IFF(access_event_type = 'OUT', access_timestamp, NULL)) AS last_out_timestamp,
                   COALESCE(SUM(IFF(access_event_type = 'IN' AND next_event_type = 'OUT' AND next_event_timestamp >= access_timestamp,
                                    DATEDIFF('minute', access_timestamp, next_event_timestamp), NULL)), 0) AS worked_minutes,
                   COUNT_IF(access_event_type = 'IN' AND next_event_type = 'OUT' AND next_event_timestamp >= access_timestamp) AS completed_pair_count,
                   COUNT_IF(access_event_type = 'IN' AND NOT COALESCE(next_event_type = 'OUT' AND next_event_timestamp >= access_timestamp, FALSE)) AS unmatched_in_count,
                   COUNT_IF(access_event_type = 'OUT' AND previous_event_type <> 'IN') + COUNT_IF(access_event_type = 'OUT' AND previous_event_type IS NULL) AS unmatched_out_count
            FROM ordered_events
            GROUP BY employee_hk, access_date
        )
        SELECT SHA2(employee_hk || '|' || access_date::VARCHAR, 256) AS employee_daily_attendance_hk,
               employee_hk, access_date, first_in_timestamp, last_out_timestamp, worked_minutes,
               completed_pair_count, unmatched_in_count, unmatched_out_count,
               (completed_pair_count > 0 AND unmatched_in_count = 0 AND unmatched_out_count = 0) AS is_complete_day
        FROM daily_rollup
    ) s
    ON t.employee_daily_attendance_hk = s.employee_daily_attendance_hk
    WHEN MATCHED THEN UPDATE SET
        first_in_timestamp = s.first_in_timestamp,
        last_out_timestamp = s.last_out_timestamp,
        worked_minutes = s.worked_minutes,
        completed_pair_count = s.completed_pair_count,
        unmatched_in_count = s.unmatched_in_count,
        unmatched_out_count = s.unmatched_out_count,
        is_complete_day = s.is_complete_day,
        __GOLD_LOAD_TS = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (employee_daily_attendance_hk, employee_hk, access_date, first_in_timestamp, last_out_timestamp, worked_minutes, completed_pair_count, unmatched_in_count, unmatched_out_count, is_complete_day, __GOLD_LOAD_TS)
    VALUES
        (s.employee_daily_attendance_hk, s.employee_hk, s.access_date, s.first_in_timestamp, s.last_out_timestamp, s.worked_minutes, s.completed_pair_count, s.unmatched_in_count, s.unmatched_out_count, s.is_complete_day, CURRENT_TIMESTAMP());
    v_rows_merged := SQLROWCOUNT;

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = 'SUCCESS', rows_processed = :v_rows_merged, end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = 'SP_LOAD_GOLD_FCT_EMPLOYEE_DAILY_ATTENDANCE' AND start_ts = :v_start_ts AND status = 'STARTED';
    RETURN 'SP_LOAD_GOLD_FCT_EMPLOYEE_DAILY_ATTENDANCE: merged ' || v_rows_merged || ' day(s).';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = 'FAILED', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = 'SP_LOAD_GOLD_FCT_EMPLOYEE_DAILY_ATTENDANCE' AND start_ts = :v_start_ts AND status = 'STARTED';
        RAISE;
END;
$$
;
-- Layer coordinators are defined last, after every callable dependency exists.
-- 3. Recreate the 2 orchestration wrappers in UTIL, repointed at UTIL ------
CREATE OR REPLACE PROCEDURE UTIL.SP_RUN_ALL_SILVER_LOADS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Orchestration wrapper: runs every SILVER load procedure (10, including the 3 skill-related entities) for the current bronze->silver batch.'
EXECUTE AS OWNER
AS '
BEGIN
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_DEPARTMENTS();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_OFFICES();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_COMPANIES();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_EMPLOYEES();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_PROJECTS();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_EMPLOYEE_PROJECT_ASSIGNMENTS();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_EMPLOYEE_DAILY_ACCESS();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_SKILLS();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_EMPLOYEE_SKILLS();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_SILVER_PROJECT_TECHNOLOGIES();
    RETURN ''SP_RUN_ALL_SILVER_LOADS: all 10 SILVER load procedures completed.'';
END;
';

CREATE OR REPLACE PROCEDURE UTIL.SP_RUN_ALL_GOLD_LOADS()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Orchestration wrapper: runs all GOLD load procedures in dependency order - parent dimensions, then child/reference dimensions, then facts/bridges that depend on them.'
EXECUTE AS OWNER
AS '
BEGIN
    -- Parent dimensions (no dependency on other dimensions).
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_DIM_DEPARTMENT();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_DIM_OFFICE();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_DIM_COMPANY();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_DIM_SKILL();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_DIM_ASSIGNMENT_ROLE();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_DIM_PROFICIENCY();
    -- Child dimensions (reference parent business keys via deterministic hash keys).
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_DIM_EMPLOYEE();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_DIM_PROJECT();
    -- Facts and bridges (reference dimension business keys; loaded last).
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_FCT_PROJECT_BUDGET_PLAN();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_FACT_EMPLOYEE_PROJECT_ASSIGNMENT();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_FACT_EMPLOYEE_DAILY_ACCESS();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_FCT_EMPLOYEE_DAILY_ATTENDANCE();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_BR_EMPLOYEE_SKILL();
    CALL HR_ANALYTICS.UTIL.SP_LOAD_GOLD_BR_PROJECT_SKILL_REQUIREMENT();
    RETURN ''SP_RUN_ALL_GOLD_LOADS: all 8 dimension, 5 fact, and 2 bridge load procedures completed.'';
END;
';



-- ============================================================================
-- SECTION: V009 - Orchestration Tasks and Delta Batch Loader
-- ============================================================================

-- V009__orchestration_and_delta_batch.sql
-- Tasks are intentionally created suspended: scheduling is an environment decision, not a migration side effect.
-- SP_APPLY_DAILY_DELTA_BATCH steps: validate day_NN; COPY each available S3
-- delta file with stage metadata; run the Silver coordinator; then run the
-- Gold coordinator; finally record either the batch summary or the error.
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE TASK HR_ANALYTICS.UTIL.TASK_BRONZE_TO_SILVER
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '60 MINUTE'
    TASK_AUTO_RETRY_ATTEMPTS = 2
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    COMMENT = 'Root task: loads new BRONZE rows into SILVER when any BRONZE stream has unconsumed data.'
    WHEN
        SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.DEPARTMENTS_STRM')
        OR SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.OFFICES_STRM')
        OR SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.COMPANIES_STRM')
        OR SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.EMPLOYEES_STRM')
        OR SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.PROJECTS_STRM')
        OR SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS_STRM')
        OR SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.EMPLOYEE_DAILY_ACCESS_STRM')
        OR SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.SKILLS_STRM')
        OR SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.EMPLOYEE_SKILLS_STRM')
        OR SYSTEM$STREAM_HAS_DATA('HR_ANALYTICS.BRONZE.PROJECT_TECHNOLOGIES_STRM')
    AS CALL HR_ANALYTICS.UTIL.SP_RUN_ALL_SILVER_LOADS();

CREATE OR REPLACE TASK HR_ANALYTICS.UTIL.TASK_SILVER_TO_GOLD
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Child task: builds the GOLD star schema (SCD2 dimensions then facts) after SILVER has finished loading.'
    AFTER HR_ANALYTICS.UTIL.TASK_BRONZE_TO_SILVER
    AS CALL HR_ANALYTICS.UTIL.SP_RUN_ALL_GOLD_LOADS();

-- Tasks are created suspended. After setting the production warehouse and
-- schedule, an operator may explicitly resume the root task.
CREATE OR REPLACE PROCEDURE UTIL.SP_APPLY_DAILY_DELTA_BATCH(V_DAY_FOLDER VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Operational batch loader for one internal-stage daily-delta folder (e.g. day_06). Reads @CSV_STAGE/daily-incremental/<day_folder>/ through CSV_STAGE; each entity COPY uses a PATTERN so absent source files are skipped. It records stage metadata, then runs Silver followed by Gold.'
EXECUTE AS OWNER
AS
$$
DECLARE
    v_start_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_err_msg VARCHAR;
    v_summary VARCHAR DEFAULT '';
    v_sql VARCHAR;
BEGIN
    IF (NOT V_DAY_FOLDER REGEXP '^day_[0-9]+$') THEN
        RETURN 'SP_APPLY_DAILY_DELTA_BATCH: rejected - day_folder must match ''day_NN'', got ''' || V_DAY_FOLDER || '''.';
    END IF;

    INSERT INTO HR_ANALYTICS.GOVERNANCE.ETL_LOG (procedure_name, target_layer, target_object, status, start_ts)
    VALUES ('SP_APPLY_DAILY_DELTA_BATCH', 'BRONZE', :V_DAY_FOLDER, 'STARTED', :v_start_ts);
    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT('INFO', 'SP_APPLY_DAILY_DELTA_BATCH started for ' || :V_DAY_FOLDER || '.');

    v_sql := 'COPY INTO HR_ANALYTICS.BRONZE.COMPANIES ' ||
             '(company_id, company_name, industry, company_country, company_classification, added_date, updated_date, is_active, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS) ' ||
             'FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME ' ||
             'FROM @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/) ' ||
             'PATTERN = ''.*' || :V_DAY_FOLDER || '/03_companies_master_delta\\.csv'' ' ||
             'FILE_FORMAT = (FORMAT_NAME = ''HR_ANALYTICS.UTIL.CSV_FF'')';
    EXECUTE IMMEDIATE v_sql;
    v_summary := v_summary || 'companies=' || SQLROWCOUNT || ' ';
    v_sql := 'COPY INTO HR_ANALYTICS.BRONZE.EMPLOYEES ' ||
             '(employee_id, access_id, employee_name, employee_email, department_id, office_id, manager_employee_id, job_title, job_level, employment_status, hire_date, added_date, updated_date, is_active, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS) ' ||
             'FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME ' ||
             'FROM @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/) ' ||
             'PATTERN = ''.*' || :V_DAY_FOLDER || '/04_employees_master_delta\\.csv'' ' ||
             'FILE_FORMAT = (FORMAT_NAME = ''HR_ANALYTICS.UTIL.CSV_FF'')';
    EXECUTE IMMEDIATE v_sql;
    v_summary := v_summary || 'employees=' || SQLROWCOUNT || ' ';
    v_sql := 'COPY INTO HR_ANALYTICS.BRONZE.PROJECTS ' ||
             '(project_id, project_name, company_id, owning_department_id, project_type, project_billing_type, project_budget_usd, project_status, start_date, planned_end_date, actual_end_date, added_date, updated_date, is_active, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS) ' ||
             'FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME ' ||
             'FROM @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/) ' ||
             'PATTERN = ''.*' || :V_DAY_FOLDER || '/05_projects_master_delta\\.csv'' ' ||
             'FILE_FORMAT = (FORMAT_NAME = ''HR_ANALYTICS.UTIL.CSV_FF'')';
    EXECUTE IMMEDIATE v_sql;
    v_summary := v_summary || 'projects=' || SQLROWCOUNT || ' ';
    v_sql := 'COPY INTO HR_ANALYTICS.BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS ' ||
             '(assignment_id, employee_id, project_id, assignment_role, allocation_percent, assignment_start_date, assignment_end_date, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS) ' ||
             'FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME ' ||
             'FROM @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/) ' ||
             'PATTERN = ''.*' || :V_DAY_FOLDER || '/06_employee_project_assignments_delta\\.csv'' ' ||
             'FILE_FORMAT = (FORMAT_NAME = ''HR_ANALYTICS.UTIL.CSV_FF'')';
    EXECUTE IMMEDIATE v_sql;
    v_summary := v_summary || 'assignments=' || SQLROWCOUNT || ' ';
    v_sql := 'COPY INTO HR_ANALYTICS.BRONZE.EMPLOYEE_DAILY_ACCESS ' ||
             '(access_event_id, access_id, office_id, access_date, access_timestamp, access_event_type, office_city, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS) ' ||
             'FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME ' ||
             'FROM @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/) ' ||
             'PATTERN = ''.*' || :V_DAY_FOLDER || '/07_employee_daily_access_delta\\.csv'' ' ||
             'FILE_FORMAT = (FORMAT_NAME = ''HR_ANALYTICS.UTIL.CSV_FF'')';
    EXECUTE IMMEDIATE v_sql;
    v_summary := v_summary || 'access=' || SQLROWCOUNT || ' ';
    v_sql := 'COPY INTO HR_ANALYTICS.BRONZE.EMPLOYEE_SKILLS ' ||
             '(employee_skill_id, employee_id, skill_id, proficiency_level, is_primary_skill, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS) ' ||
             'FROM (SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME ' ||
             'FROM @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/) ' ||
             'PATTERN = ''.*' || :V_DAY_FOLDER || '/09_employee_skills_delta\\.csv'' ' ||
             'FILE_FORMAT = (FORMAT_NAME = ''HR_ANALYTICS.UTIL.CSV_FF'')';
    EXECUTE IMMEDIATE v_sql;
    v_summary := v_summary || 'employee_skills=' || SQLROWCOUNT || ' ';
    v_sql := 'COPY INTO HR_ANALYTICS.BRONZE.PROJECT_TECHNOLOGIES ' ||
             '(project_technology_id, project_id, skill_id, required_proficiency_level, is_primary_technology, __SRC_DELTA_DATE, __SRC_OPERATION_TYPE, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS) ' ||
             'FROM (SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME ' ||
             'FROM @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/) ' ||
             'PATTERN = ''.*' || :V_DAY_FOLDER || '/10_project_technologies_delta\\.csv'' ' ||
             'FILE_FORMAT = (FORMAT_NAME = ''HR_ANALYTICS.UTIL.CSV_FF'')';
    EXECUTE IMMEDIATE v_sql;
    v_summary := v_summary || 'project_technologies=' || SQLROWCOUNT || ' ';

    CALL HR_ANALYTICS.UTIL.SP_RUN_ALL_SILVER_LOADS();
    CALL HR_ANALYTICS.UTIL.SP_RUN_ALL_GOLD_LOADS();

    UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
       SET status = 'SUCCESS', end_ts = CURRENT_TIMESTAMP(),
           duration_ms = DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP())
     WHERE procedure_name = 'SP_APPLY_DAILY_DELTA_BATCH' AND start_ts = :v_start_ts AND status = 'STARTED';

    CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT('INFO', 'SP_APPLY_DAILY_DELTA_BATCH succeeded for ' || :V_DAY_FOLDER || ': ' || v_summary);
    RETURN 'SP_APPLY_DAILY_DELTA_BATCH: ' || :V_DAY_FOLDER || ' -> ' || v_summary || '; silver+gold reloaded.';
EXCEPTION
    WHEN OTHER THEN
        v_err_msg := SQLERRM;
        UPDATE HR_ANALYTICS.GOVERNANCE.ETL_LOG
           SET status = 'FAILED', error_message = :v_err_msg, end_ts = CURRENT_TIMESTAMP(),
               duration_ms = DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP())
         WHERE procedure_name = 'SP_APPLY_DAILY_DELTA_BATCH' AND start_ts = :v_start_ts AND status = 'STARTED';
        CALL HR_ANALYTICS.GOVERNANCE.SP_LOG_EVENT('ERROR', 'SP_APPLY_DAILY_DELTA_BATCH failed for ' || :V_DAY_FOLDER || ': ' || v_err_msg);
        RAISE;
END;
$$
;



-- ============================================================================
-- SECTION: V010 - Governance Tags, Semantic View, Data Metric Functions
-- ============================================================================

-- V010__governance_semantic_and_metrics.sql
-- Apply governance only after all target objects exist. Data metric functions are additive observability configuration.
USE ROLE ACCOUNTADMIN;
USE DATABASE HR_ANALYTICS;

-- V015__apply_governance_tags.sql
-- Purpose: Apply the GOVERNANCE tags created in V002 to concrete SILVER and
--          GOLD objects: business-domain tagging at the table level, and
--          PII / data-classification tagging at the column level for the
--          two columns that carry personal data (employee name, employee
--          email).
-- Layer:   Governance
-- ---------------------------------------------------------------------------


-- Table-level domain tagging -------------------------------------------------
ALTER TABLE SILVER.DEPARTMENTS SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'HR';
ALTER TABLE SILVER.OFFICES SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'HR';
ALTER TABLE SILVER.COMPANIES SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'PROJECT';
ALTER TABLE SILVER.EMPLOYEES SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'HR';
ALTER TABLE SILVER.PROJECTS SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'PROJECT';
ALTER TABLE SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'PROJECT';
ALTER TABLE SILVER.EMPLOYEE_DAILY_ACCESS SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'ACCESS';

ALTER TABLE GOLD.DIM_DEPARTMENT SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'HR';
ALTER TABLE GOLD.DIM_OFFICE SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'HR';
ALTER TABLE GOLD.DIM_COMPANY SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'PROJECT';
ALTER TABLE GOLD.DIM_EMPLOYEE SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'HR';
ALTER TABLE GOLD.DIM_PROJECT SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'PROJECT';
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'PROJECT';
ALTER TABLE GOLD.FACT_EMPLOYEE_DAILY_ACCESS SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'ACCESS';
ALTER TABLE GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE SET TAG GOVERNANCE.DATA_DOMAIN_TAG = 'ACCESS';

-- Column-level PII / classification tagging ----------------------------------
ALTER TABLE SILVER.EMPLOYEES MODIFY COLUMN employee_name
    SET TAG GOVERNANCE.PII_TAG = 'NAME', GOVERNANCE.DATA_CLASSIFICATION_TAG = 'CONFIDENTIAL';
ALTER TABLE SILVER.EMPLOYEES MODIFY COLUMN employee_email
    SET TAG GOVERNANCE.PII_TAG = 'EMAIL', GOVERNANCE.DATA_CLASSIFICATION_TAG = 'CONFIDENTIAL';

ALTER TABLE GOLD.DIM_EMPLOYEE MODIFY COLUMN employee_name
    SET TAG GOVERNANCE.PII_TAG = 'NAME', GOVERNANCE.DATA_CLASSIFICATION_TAG = 'CONFIDENTIAL';
ALTER TABLE GOLD.DIM_EMPLOYEE MODIFY COLUMN employee_email
    SET TAG GOVERNANCE.PII_TAG = 'EMAIL', GOVERNANCE.DATA_CLASSIFICATION_TAG = 'CONFIDENTIAL';
-- V017__create_data_metric_functions.sql
-- Purpose: Basic, layer-appropriate data-quality monitoring using Snowflake
--          system Data Metric Functions (DMFs). Design:
--            BRONZE/SILVER (append-only, historical revisions expected on
--            master/dimension entities): ROW_COUNT, NULL_COUNT on business
--            keys, FRESHNESS on the load-timestamp audit column. DUPLICATE_COUNT
--            is only applied to pure-event tables (assignments, daily access)
--            where the business key must never repeat - NOT to master tables,
--            since a legitimate SCD-style update intentionally re-lands the
--            same business key with a new row.
--            GOLD: ROW_COUNT, NULL_COUNT on hash keys (a NULL hash key would
--            indicate a broken join/lookup), DUPLICATE_COUNT on fact business
--            keys (facts must be unique), REFERENTIAL_INTEGRITY_COUNT on
--            fact FK hash keys against their dimension (catches orphaned
--            rows at the exact same boundaries as the declared FK
--            constraints), and FRESHNESS on the gold load timestamp.
--            FRESHNESS uses the zero-argument, table-level system DMF
--            (FRESHNESS() - based on the table's last-modified metadata)
--            because the column-based FRESHNESS(TABLE(...)) overload does
--            not support TIMESTAMP_NTZ, which is the type used by our
--            __STG_LOAD_TS / __SILVER_LOAD_TS / __GOLD_LOAD_TS audit columns.
-- Layer:   Governance / data quality
-- Schedule: TRIGGER_ON_CHANGES at the schema level - each table gets scanned
--          shortly after its underlying data changes, without a fixed cron.
-- Note:    Each ALTER TABLE ADD DATA METRIC FUNCTION statement binds exactly
--          one DMF; unlike CREATE TABLE ... WITH DATA METRIC FUNCTION, ALTER
--          TABLE does not accept a comma-separated list of bindings.
-- ---------------------------------------------------------------------------


ALTER SCHEMA BRONZE SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER SCHEMA SILVER SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER SCHEMA GOLD SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- ============================================================================
-- BRONZE
-- ============================================================================
ALTER TABLE BRONZE.DEPARTMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.DEPARTMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (department_id);
ALTER TABLE BRONZE.DEPARTMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE BRONZE.OFFICES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.OFFICES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (office_id);
ALTER TABLE BRONZE.OFFICES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE BRONZE.COMPANIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.COMPANIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (company_id);
ALTER TABLE BRONZE.COMPANIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE BRONZE.EMPLOYEES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.EMPLOYEES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_id);
ALTER TABLE BRONZE.EMPLOYEES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE BRONZE.PROJECTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.PROJECTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (project_id);
ALTER TABLE BRONZE.PROJECTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (assignment_id);
ALTER TABLE BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (assignment_id);
ALTER TABLE BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE BRONZE.EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (access_event_id);
ALTER TABLE BRONZE.EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (access_event_id);
ALTER TABLE BRONZE.EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

-- ============================================================================
-- SILVER
-- ============================================================================
ALTER TABLE SILVER.DEPARTMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.DEPARTMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (department_id);
ALTER TABLE SILVER.DEPARTMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE SILVER.OFFICES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.OFFICES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (office_id);
ALTER TABLE SILVER.OFFICES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE SILVER.COMPANIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.COMPANIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (company_id);
ALTER TABLE SILVER.COMPANIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE SILVER.EMPLOYEES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.EMPLOYEES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_id);
ALTER TABLE SILVER.EMPLOYEES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE SILVER.PROJECTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.PROJECTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (project_id);
ALTER TABLE SILVER.PROJECTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (assignment_id);
ALTER TABLE SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (assignment_id);
ALTER TABLE SILVER.EMPLOYEE_PROJECT_ASSIGNMENTS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE SILVER.EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (access_event_id);
ALTER TABLE SILVER.EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (access_event_id);
ALTER TABLE SILVER.EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

-- ============================================================================
-- GOLD - dimensions
-- ============================================================================
ALTER TABLE GOLD.DIM_DEPARTMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.DIM_DEPARTMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (department_hk);
ALTER TABLE GOLD.DIM_DEPARTMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.DIM_OFFICE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.DIM_OFFICE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (office_hk);
ALTER TABLE GOLD.DIM_OFFICE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.DIM_COMPANY ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.DIM_COMPANY ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (company_hk);
ALTER TABLE GOLD.DIM_COMPANY ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.DIM_EMPLOYEE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.DIM_EMPLOYEE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_hk);
ALTER TABLE GOLD.DIM_EMPLOYEE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.DIM_PROJECT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.DIM_PROJECT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (project_hk);
ALTER TABLE GOLD.DIM_PROJECT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

-- ============================================================================
-- GOLD - facts (uniqueness + referential integrity against parent dimensions)
-- ============================================================================
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (assignment_id);
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_hk);
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (project_hk);
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (employee_hk, TABLE(HR_ANALYTICS.GOLD.DIM_EMPLOYEE(employee_hk)));
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (project_hk, TABLE(HR_ANALYTICS.GOLD.DIM_PROJECT(project_hk)));
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.FACT_EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.FACT_EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (access_event_id);
ALTER TABLE GOLD.FACT_EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_hk);
ALTER TABLE GOLD.FACT_EMPLOYEE_DAILY_ACCESS
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (office_hk, TABLE(HR_ANALYTICS.GOLD.DIM_OFFICE(office_hk)));
ALTER TABLE GOLD.FACT_EMPLOYEE_DAILY_ACCESS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

-- Daily attendance is a derived aggregate: one row per employee/date, so its
-- hash key must be unique and worked_minutes must be populated.
ALTER TABLE GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (employee_daily_attendance_hk);
ALTER TABLE GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_hk);
ALTER TABLE GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (worked_minutes);
ALTER TABLE GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();
-- V030__create_phase3_data_metric_functions.sql
-- Purpose: Data-quality DMFs for every Phase 3 new/changed object, following
--          the same layer-appropriate pattern established in V017:
--            BRONZE/SILVER master-type tables (SKILLS, EMPLOYEE_SKILLS,
--              PROJECT_TECHNOLOGIES): ROW_COUNT, NULL_COUNT on the natural
--              key, FRESHNESS. No DUPLICATE_COUNT - these legitimately
--              re-land the same natural key on every delta update.
--            GOLD reference dimensions (DIM_SKILL, DIM_ASSIGNMENT_ROLE,
--              DIM_PROFICIENCY): ROW_COUNT, NULL_COUNT on the hash key,
--              FRESHNESS.
--            DIM_DATE (static Type 0, never refreshed after V024): ROW_COUNT,
--              NULL_COUNT, DUPLICATE_COUNT on date_day - no FRESHNESS, since
--              a static calendar spine is expected to never change again.
--            GOLD effective-dated facts/bridges (FACT_EMPLOYEE_PROJECT_ASSIGNMENT,
--              FCT_PROJECT_BUDGET_PLAN, BR_EMPLOYEE_SKILL,
--              BR_PROJECT_SKILL_REQUIREMENT): ROW_COUNT, DUPLICATE_COUNT on
--              the version-level hash key (never on the business key, which
--              legitimately repeats across versions), NULL_COUNT + 
--              REFERENTIAL_INTEGRITY_COUNT on each FK hash key, FRESHNESS.
-- Layer:   Governance / data quality
-- Note:    FACT_EMPLOYEE_PROJECT_ASSIGNMENT was recreated in V027 with a new
--          shape, so it has no pre-existing DMF bindings to replace here.
-- ---------------------------------------------------------------------------

-- The semantic model deliberately exposes both grains. Use the event fact for
-- entry/exit audit questions; use the daily fact for duration totals. This
-- prevents summing event rows or treating first-IN to last-OUT as work time.
CREATE OR REPLACE SEMANTIC VIEW HR_ANALYTICS.GOLD.HR_ANALYTICS_MODEL
    TABLES (
        HR_ANALYTICS.GOLD.DIM_EMPLOYEE PRIMARY KEY (EMPLOYEE_HK),
        HR_ANALYTICS.GOLD.DIM_OFFICE PRIMARY KEY (OFFICE_HK),
        HR_ANALYTICS.GOLD.FACT_EMPLOYEE_DAILY_ACCESS PRIMARY KEY (ACCESS_EVENT_HK),
        HR_ANALYTICS.GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE PRIMARY KEY (EMPLOYEE_DAILY_ATTENDANCE_HK)
    )
    RELATIONSHIPS (
        FACT_ACCESS_TO_EMPLOYEE AS FACT_EMPLOYEE_DAILY_ACCESS(EMPLOYEE_HK) REFERENCES DIM_EMPLOYEE(EMPLOYEE_HK),
        FACT_ACCESS_TO_OFFICE AS FACT_EMPLOYEE_DAILY_ACCESS(OFFICE_HK) REFERENCES DIM_OFFICE(OFFICE_HK),
        FCT_ATTENDANCE_TO_EMPLOYEE AS FCT_EMPLOYEE_DAILY_ATTENDANCE(EMPLOYEE_HK) REFERENCES DIM_EMPLOYEE(EMPLOYEE_HK)
    )
    FACTS (
        FACT_EMPLOYEE_DAILY_ACCESS.ACCESS_EVENT_ID AS ACCESS_EVENT_ID,
        FCT_EMPLOYEE_DAILY_ATTENDANCE.WORKED_MINUTES AS WORKED_MINUTES,
        FCT_EMPLOYEE_DAILY_ATTENDANCE.COMPLETED_PAIR_COUNT AS COMPLETED_PAIR_COUNT,
        FCT_EMPLOYEE_DAILY_ATTENDANCE.UNMATCHED_IN_COUNT AS UNMATCHED_IN_COUNT,
        FCT_EMPLOYEE_DAILY_ATTENDANCE.UNMATCHED_OUT_COUNT AS UNMATCHED_OUT_COUNT
    )
    DIMENSIONS (
        DIM_EMPLOYEE.EMPLOYEE_ID AS EMPLOYEE_ID,
        DIM_EMPLOYEE.EMPLOYEE_NAME AS EMPLOYEE_NAME,
        DIM_EMPLOYEE.JOB_TITLE AS JOB_TITLE,
        DIM_OFFICE.OFFICE_CITY AS OFFICE_CITY,
        DIM_OFFICE.OFFICE_COUNTRY AS OFFICE_COUNTRY,
        FACT_EMPLOYEE_DAILY_ACCESS.ACCESS_EVENT_TYPE AS ACCESS_EVENT_TYPE,
        FACT_EMPLOYEE_DAILY_ACCESS.ACCESS_EVENT_DATE AS ACCESS_DATE,
        FCT_EMPLOYEE_DAILY_ATTENDANCE.ATTENDANCE_DATE AS ACCESS_DATE,
        FCT_EMPLOYEE_DAILY_ATTENDANCE.IS_COMPLETE_DAY AS IS_COMPLETE_DAY
    )
    COMMENT = 'HR access semantic model. FACT_EMPLOYEE_DAILY_ACCESS is atomic IN/OUT evidence; FCT_EMPLOYEE_DAILY_ATTENDANCE is the employee/date duration aggregate based on valid adjacent IN-to-OUT pairs.';

-- ============================================================================
-- BRONZE - 3 new entities
-- ============================================================================
ALTER TABLE BRONZE.SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (skill_id);
ALTER TABLE BRONZE.SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE BRONZE.EMPLOYEE_SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.EMPLOYEE_SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_skill_id);
ALTER TABLE BRONZE.EMPLOYEE_SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE BRONZE.PROJECT_TECHNOLOGIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE BRONZE.PROJECT_TECHNOLOGIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (project_technology_id);
ALTER TABLE BRONZE.PROJECT_TECHNOLOGIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

-- ============================================================================
-- SILVER - 3 new entities
-- ============================================================================
ALTER TABLE SILVER.SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (skill_id);
ALTER TABLE SILVER.SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE SILVER.EMPLOYEE_SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.EMPLOYEE_SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_skill_id);
ALTER TABLE SILVER.EMPLOYEE_SKILLS ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE SILVER.PROJECT_TECHNOLOGIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE SILVER.PROJECT_TECHNOLOGIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (project_technology_id);
ALTER TABLE SILVER.PROJECT_TECHNOLOGIES ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

-- ============================================================================
-- GOVERNANCE - quarantine log (row count + freshness only; no natural key to
-- null-check since every row is, by definition, a rejected/irregular record)
-- ============================================================================
ALTER SCHEMA GOVERNANCE SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER TABLE GOVERNANCE.QUARANTINE_LOG ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOVERNANCE.QUARANTINE_LOG ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

-- ============================================================================
-- GOLD - new reference dimensions
-- ============================================================================
ALTER TABLE GOLD.DIM_SKILL ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.DIM_SKILL ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (skill_hk);
ALTER TABLE GOLD.DIM_SKILL ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.DIM_ASSIGNMENT_ROLE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.DIM_ASSIGNMENT_ROLE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (assignment_role_hk);
ALTER TABLE GOLD.DIM_ASSIGNMENT_ROLE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.DIM_PROFICIENCY ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.DIM_PROFICIENCY ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (proficiency_hk);
ALTER TABLE GOLD.DIM_PROFICIENCY ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

-- DIM_DATE is a static Type-0 calendar spine, generated once in V024 and not
-- expected to change again - no FRESHNESS DMF, but uniqueness/null checks
-- still apply.
ALTER TABLE GOLD.DIM_DATE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.DIM_DATE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (date_hk);
ALTER TABLE GOLD.DIM_DATE ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (date_day);

-- ============================================================================
-- GOLD - effective-dated fact/bridges (version-level uniqueness, business-key
-- FK referential integrity)
-- ============================================================================
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (assignment_version_hk);
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_hk);
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (project_hk);
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (employee_hk, TABLE(HR_ANALYTICS.GOLD.DIM_EMPLOYEE(employee_hk)));
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (project_hk, TABLE(HR_ANALYTICS.GOLD.DIM_PROJECT(project_hk)));
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (assignment_role_hk, TABLE(HR_ANALYTICS.GOLD.DIM_ASSIGNMENT_ROLE(assignment_role_hk)));
ALTER TABLE GOLD.FACT_EMPLOYEE_PROJECT_ASSIGNMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.FCT_PROJECT_BUDGET_PLAN ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.FCT_PROJECT_BUDGET_PLAN ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (project_budget_version_hk);
ALTER TABLE GOLD.FCT_PROJECT_BUDGET_PLAN ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (project_hk);
ALTER TABLE GOLD.FCT_PROJECT_BUDGET_PLAN
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (project_hk, TABLE(HR_ANALYTICS.GOLD.DIM_PROJECT(project_hk)));
ALTER TABLE GOLD.FCT_PROJECT_BUDGET_PLAN
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (company_hk, TABLE(HR_ANALYTICS.GOLD.DIM_COMPANY(company_hk)));
ALTER TABLE GOLD.FCT_PROJECT_BUDGET_PLAN ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.BR_EMPLOYEE_SKILL ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.BR_EMPLOYEE_SKILL ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (employee_skill_version_hk);
ALTER TABLE GOLD.BR_EMPLOYEE_SKILL ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (employee_hk);
ALTER TABLE GOLD.BR_EMPLOYEE_SKILL ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (skill_hk);
ALTER TABLE GOLD.BR_EMPLOYEE_SKILL
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (employee_hk, TABLE(HR_ANALYTICS.GOLD.DIM_EMPLOYEE(employee_hk)));
ALTER TABLE GOLD.BR_EMPLOYEE_SKILL
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (skill_hk, TABLE(HR_ANALYTICS.GOLD.DIM_SKILL(skill_hk)));
ALTER TABLE GOLD.BR_EMPLOYEE_SKILL ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();

ALTER TABLE GOLD.BR_PROJECT_SKILL_REQUIREMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE GOLD.BR_PROJECT_SKILL_REQUIREMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (project_technology_version_hk);
ALTER TABLE GOLD.BR_PROJECT_SKILL_REQUIREMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (project_hk);
ALTER TABLE GOLD.BR_PROJECT_SKILL_REQUIREMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (skill_hk);
ALTER TABLE GOLD.BR_PROJECT_SKILL_REQUIREMENT
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (project_hk, TABLE(HR_ANALYTICS.GOLD.DIM_PROJECT(project_hk)));
ALTER TABLE GOLD.BR_PROJECT_SKILL_REQUIREMENT
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.REFERENTIAL_INTEGRITY_COUNT
      ON (skill_hk, TABLE(HR_ANALYTICS.GOLD.DIM_SKILL(skill_hk)));
ALTER TABLE GOLD.BR_PROJECT_SKILL_REQUIREMENT ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ();
-- V018__create_semantic_view.sql
-- Purpose: Cortex Analyst semantic view over the GOLD star schema, enabling
--          natural-language questions about departments, offices, companies,
--          employees, and projects (SCD2 dimensions), plus employee-project
--          assignment and daily badge-access facts. Generated via
--          `cortex agent-studio` (sv-generate/sv-write/sv-deploy) from the
--          GOLD tables and a set of seed verified queries; this file is the
--          resulting DDL captured for repeatable deployment.
-- Layer:   Semantic / consumption
-- ---------------------------------------------------------------------------



