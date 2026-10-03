-- ============================================================================
-- put_delta_load.sql
-- Upload daily-delta CSV files to the internal stage and run the batch loader
-- for each available day folder (day_01 through day_05).
--
-- Prerequisites:
--   1. hr_analytics_deploy.sql has been executed (all objects exist)
--   2. put_full_load.sql has been executed (base data loaded)
--   3. Delta CSV files are at git-repo/daily-delta/day_NN/
--
-- Usage (SnowSQL or Python connector - PUT requires client-side driver):
--   snowsql -c <connection> -f put_delta_load.sql
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HR_ANALYTICS;
USE SCHEMA UTIL;

-- ============================================================================
-- DAY 01
-- ============================================================================
PUT file://git-repo/daily-delta/day_01/03_companies_master_delta.csv       @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_01/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_01/04_employees_master_delta.csv       @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_01/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_01/05_projects_master_delta.csv        @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_01/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_01/09_employee_skills_delta.csv        @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_01/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_01/10_project_technologies_delta.csv   @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_01/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CALL HR_ANALYTICS.UTIL.SP_APPLY_DAILY_DELTA_BATCH('day_01');

-- ============================================================================
-- DAY 02
-- ============================================================================
PUT file://git-repo/daily-delta/day_02/03_companies_master_delta.csv       @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_02/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_02/05_projects_master_delta.csv        @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_02/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_02/07_employee_daily_access_delta.csv  @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_02/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CALL HR_ANALYTICS.UTIL.SP_APPLY_DAILY_DELTA_BATCH('day_02');

-- ============================================================================
-- DAY 03
-- ============================================================================
PUT file://git-repo/daily-delta/day_03/04_employees_master_delta.csv       @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_03/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_03/06_employee_project_assignments_delta.csv @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_03/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_03/07_employee_daily_access_delta.csv  @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_03/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_03/09_employee_skills_delta.csv        @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_03/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CALL HR_ANALYTICS.UTIL.SP_APPLY_DAILY_DELTA_BATCH('day_03');

-- ============================================================================
-- DAY 04
-- ============================================================================
PUT file://git-repo/daily-delta/day_04/04_employees_master_delta.csv       @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_04/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_04/05_projects_master_delta.csv        @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_04/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_04/06_employee_project_assignments_delta.csv @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_04/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CALL HR_ANALYTICS.UTIL.SP_APPLY_DAILY_DELTA_BATCH('day_04');

-- ============================================================================
-- DAY 05
-- ============================================================================
PUT file://git-repo/daily-delta/day_05/04_employees_master_delta.csv       @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_05/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_05/05_projects_master_delta.csv        @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_05/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_05/06_employee_project_assignments_delta.csv @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_05/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_05/07_employee_daily_access_delta.csv  @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_05/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://git-repo/daily-delta/day_05/09_employee_skills_delta.csv        @HR_ANALYTICS.UTIL.CSV_STAGE/daily-incremental/day_05/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CALL HR_ANALYTICS.UTIL.SP_APPLY_DAILY_DELTA_BATCH('day_05');

-- ============================================================================
-- Verify: check row counts grew and quarantine is clean
-- ============================================================================

SELECT 'BRONZE' AS layer, 'EMPLOYEES' AS tbl, COUNT(*) AS row_count FROM HR_ANALYTICS.BRONZE.EMPLOYEES
UNION ALL SELECT 'BRONZE', 'PROJECTS', COUNT(*) FROM HR_ANALYTICS.BRONZE.PROJECTS
UNION ALL SELECT 'BRONZE', 'DAILY_ACCESS', COUNT(*) FROM HR_ANALYTICS.BRONZE.EMPLOYEE_DAILY_ACCESS
UNION ALL SELECT 'GOLD', 'DIM_EMPLOYEE', COUNT(*) FROM HR_ANALYTICS.GOLD.DIM_EMPLOYEE
UNION ALL SELECT 'GOLD', 'FACT_ACCESS', COUNT(*) FROM HR_ANALYTICS.GOLD.FACT_EMPLOYEE_DAILY_ACCESS
UNION ALL SELECT 'GOLD', 'FCT_ATTENDANCE', COUNT(*) FROM HR_ANALYTICS.GOLD.FCT_EMPLOYEE_DAILY_ATTENDANCE
UNION ALL SELECT 'QUARANTINE', 'LOG', COUNT(*) FROM HR_ANALYTICS.GOVERNANCE.QUARANTINE_LOG
ORDER BY 1, 2;

SELECT procedure_name, status, rows_processed, duration_ms
FROM HR_ANALYTICS.GOVERNANCE.ETL_LOG
WHERE procedure_name = 'SP_APPLY_DAILY_DELTA_BATCH'
ORDER BY start_ts;
