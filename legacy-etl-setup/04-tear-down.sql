-- ============================================================================
-- tear-down.sql
-- Completely removes the HR_ANALYTICS database and all child objects.
--
-- WARNING: This is destructive and irreversible. All data, procedures, stages,
--          streams, tasks, semantic views, tags, and DMFs will be dropped.
--
-- Usage:
--   snow sql -f tear-down.sql -c dbt-account
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Suspend tasks before dropping to avoid orphaned task runs.
ALTER TASK IF EXISTS HR_ANALYTICS.UTIL.TASK_SILVER_TO_GOLD SUSPEND;
ALTER TASK IF EXISTS HR_ANALYTICS.UTIL.TASK_BRONZE_TO_SILVER SUSPEND;

-- Drop the entire database (CASCADE is implicit for DROP DATABASE).
-- This removes all schemas, tables, views, streams, stages, procedures,
-- functions, tasks, tags, semantic views, and data metric function bindings.
DROP DATABASE IF EXISTS HR_ANALYTICS;
