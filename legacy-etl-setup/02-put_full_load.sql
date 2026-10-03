-- ============================================================================
-- put_full_load.sql
-- Upload base-load CSV files to the internal stage and COPY into Bronze,
-- then run Silver and Gold pipelines.
--
-- Prerequisites:
--   1. hr_analytics_deploy.sql has been executed (all objects exist)
--   2. CSV files are at the paths referenced below (git-repo/data/)
--
-- Usage (SnowSQL or snow CLI):
--   snowsql -c <connection> -f put_full_load.sql
--   -- OR via Python connector (PUT requires a client-side driver):
--   python3 run_put_full_load.py
--
-- NOTE: PUT is a client-side command and cannot run via Snowflake worksheets
--       or the SQL API. It must be executed through SnowSQL, the Python
--       connector, or another driver that supports file transfer.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HR_ANALYTICS;
USE SCHEMA UTIL;

-- ============================================================================
-- STEP 1: PUT local CSV files to the internal stage under full-load/ prefix.
--         Adjust the file:// paths if your CSV files are in a different location.
-- ============================================================================

PUT file://git-repo/data/01_departments_master.csv         @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/data/02_offices_master.csv             @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/data/03_companies_master.csv           @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/data/04_employees_master.csv           @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/data/05_projects_master.csv            @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/data/06_employee_project_assignments.csv @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/data/07_employee_daily_access.csv      @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/data/08_skills_master.csv              @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/data/09_employee_skills.csv            @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/data/10_project_technologies.csv       @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- ============================================================================
-- STEP 2: COPY staged files into Bronze tables.
-- ============================================================================

USE SCHEMA BRONZE;

COPY INTO DEPARTMENTS (department_id, department_code, department_name, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*01_departments_master\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO OFFICES (office_id, office_code, office_city, office_country, office_region, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*02_offices_master\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO COMPANIES (company_id, company_name, industry, company_country, company_classification, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*03_companies_master\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO EMPLOYEES (employee_id, access_id, employee_name, employee_email, department_id, office_id, manager_employee_id, job_title, job_level, employment_status, hire_date, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*04_employees_master\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO PROJECTS (project_id, project_name, company_id, owning_department_id, project_type, project_billing_type, project_budget_usd, project_status, start_date, planned_end_date, actual_end_date, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*05_projects_master\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO EMPLOYEE_PROJECT_ASSIGNMENTS (assignment_id, employee_id, project_id, assignment_role, allocation_percent, assignment_start_date, assignment_end_date, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*06_employee_project_assignments\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO EMPLOYEE_DAILY_ACCESS (access_event_id, access_id, office_id, access_date, access_timestamp, access_event_type, office_city, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*07_employee_daily_access\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO SKILLS (skill_id, skill_name, skill_category, added_date, updated_date, is_active, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, $6, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*08_skills_master\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO EMPLOYEE_SKILLS (employee_skill_id, employee_id, skill_id, proficiency_level, is_primary_skill, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*09_employee_skills\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

COPY INTO PROJECT_TECHNOLOGIES (project_technology_id, project_id, skill_id, required_proficiency_level, is_primary_technology, __STG_FILE_NAME, __STG_FILE_ROW_NUMBER, __STG_LOAD_TS)
FROM (SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME FROM @HR_ANALYTICS.UTIL.CSV_STAGE/full-load/)
PATTERN = '.*10_project_technologies\.csv(\.gz)?'
FILE_FORMAT = (FORMAT_NAME = 'HR_ANALYTICS.UTIL.CSV_FF');

-- ============================================================================
-- STEP 3: Run Silver and Gold pipelines.
-- ============================================================================

CALL HR_ANALYTICS.UTIL.SP_RUN_ALL_SILVER_LOADS();
CALL HR_ANALYTICS.UTIL.SP_RUN_ALL_GOLD_LOADS();

-- ============================================================================
-- STEP 4: Verify row counts.
-- ============================================================================

SELECT 'BRONZE' AS layer, 'DEPARTMENTS' AS tbl, COUNT(*) AS row_count FROM HR_ANALYTICS.BRONZE.DEPARTMENTS
UNION ALL SELECT 'BRONZE', 'OFFICES', COUNT(*) FROM HR_ANALYTICS.BRONZE.OFFICES
UNION ALL SELECT 'BRONZE', 'COMPANIES', COUNT(*) FROM HR_ANALYTICS.BRONZE.COMPANIES
UNION ALL SELECT 'BRONZE', 'EMPLOYEES', COUNT(*) FROM HR_ANALYTICS.BRONZE.EMPLOYEES
UNION ALL SELECT 'BRONZE', 'PROJECTS', COUNT(*) FROM HR_ANALYTICS.BRONZE.PROJECTS
UNION ALL SELECT 'BRONZE', 'ASSIGNMENTS', COUNT(*) FROM HR_ANALYTICS.BRONZE.EMPLOYEE_PROJECT_ASSIGNMENTS
UNION ALL SELECT 'BRONZE', 'DAILY_ACCESS', COUNT(*) FROM HR_ANALYTICS.BRONZE.EMPLOYEE_DAILY_ACCESS
UNION ALL SELECT 'BRONZE', 'SKILLS', COUNT(*) FROM HR_ANALYTICS.BRONZE.SKILLS
UNION ALL SELECT 'BRONZE', 'EMP_SKILLS', COUNT(*) FROM HR_ANALYTICS.BRONZE.EMPLOYEE_SKILLS
UNION ALL SELECT 'BRONZE', 'PROJ_TECH', COUNT(*) FROM HR_ANALYTICS.BRONZE.PROJECT_TECHNOLOGIES
UNION ALL SELECT 'GOLD', 'DIM_EMPLOYEE', COUNT(*) FROM HR_ANALYTICS.GOLD.DIM_EMPLOYEE
UNION ALL SELECT 'GOLD', 'DIM_DATE', COUNT(*) FROM HR_ANALYTICS.GOLD.DIM_DATE
UNION ALL SELECT 'GOLD', 'FACT_ACCESS', COUNT(*) FROM HR_ANALYTICS.GOLD.FACT_EMPLOYEE_DAILY_ACCESS
UNION ALL SELECT 'GOLD', 'FCT_ATTENDANCE', COUNT(*) FROM HR_ANALYTICS.GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE
ORDER BY 1, 2;
