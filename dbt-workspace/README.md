# dbt HR Analytics Pipeline

A medallion-architecture (Bronze → Silver → Gold) dbt project that transforms raw HR CSV data into a dimensional star schema in Snowflake, targeting the `DBT_HR_ANALYTICS` database.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Design Approach](#design-approach)
- [Database & Schema Layout](#database--schema-layout)
- [Source Data (Bronze Layer)](#source-data-bronze-layer)
- [Silver Layer](#silver-layer)
- [Gold Layer](#gold-layer)
- [Seeds](#seeds)
- [Snapshots](#snapshots)
- [Macros](#macros)
- [Testing Strategy](#testing-strategy)
- [Semantic View](#semantic-view)
- [Data Governance](#data-governance)
- [How to Run](#how-to-run)
- [Project Configuration](#project-configuration)
- [DAG Overview](#dag-overview)

---

## Architecture Overview

```
                          ┌─────────────────────┐
                          │   S3 External Stage  │
                          │  (CSV source files)  │
                          └─────────┬───────────┘
                                    │
                    COPY INTO via load_bronze / load_bronze_deltas
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────┐
│  BRONZE SCHEMA  (raw landing zone)                                    │
│                                                                       │
│  10 VARCHAR-typed tables, append-only.                                │
│  Includes METADATA$FILENAME and METADATA$START_SCAN_TIME for lineage. │
│  Base load + 5 daily incremental delta folders.                       │
└───────────────────────────────┬───────────────────────────────────────┘
                                │
                      dbt models (ref → source)
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────────┐
│  SILVER SCHEMA  (typed, cleansed, validated)                          │
│                                                                       │
│  10 transient tables. Type casting, trimming, null filtering,         │
│  referential integrity via subquery filters, country standardization  │
│  via seed, derived enrichment columns, delta day extraction.          │
└───────────────────────────────┬───────────────────────────────────────┘
                                │
                   dbt models (ref → silver models)
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────────┐
│  GOLD SCHEMA  (dimensional star schema)                               │
│                                                                       │
│  9 dimensions (5 SCD-2, 1 Type-1, 2 reference, 1 generated)          │
│  3 facts / 2 bridges with version history                             │
│  Hash-key surrogate keys (SHA-256)                                    │
│  SCD Type-2 via hash-based change detection on delta day files        │
├───────────────────────────────────────────────────────────────────────┤
│  SEEDS: country_mapping, proficiency_levels                           │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│  SNAPSHOTS SCHEMA                                                     │
│                                                                       │
│  snap_skills: dbt-managed SCD-2 history for skill attribute changes   │
│  (check strategy on SKILL_NAME, SKILL_CATEGORY, IS_ACTIVE)           │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│  GOVERNANCE SCHEMA                                                    │
│                                                                       │
│  Tags: ENV_TAG, DATA_CLASSIFICATION_TAG, PII_TAG, DATA_DOMAIN_TAG    │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Design Approach

### Medallion Architecture

The project follows a **three-layer medallion architecture** (Bronze → Silver → Gold), a proven pattern for data lakehouse designs:

- **Bronze**: Raw data ingested as-is from CSV files. All columns are VARCHAR to avoid load-time failures. Append-only -- deltas are added alongside base records, preserving full history.
- **Silver**: Cleansed, typed, and validated data. This layer enforces data quality at the boundary -- malformed records are filtered out, not patched. Referential integrity is enforced via subquery filters rather than hard joins, ensuring only valid foreign keys pass through.
- **Gold**: Business-ready dimensional star schema. Optimized for analytical queries with surrogate hash keys, SCD Type-2 history, and conformed dimensions.

### Why Medallion over Direct ELT?

1. **Debuggability**: Each layer is independently queryable. When a Gold metric looks wrong, you can trace it back through Silver to the raw Bronze record.
2. **Reprocessability**: Bronze is immutable. If Silver logic changes, you re-run from Bronze without re-ingesting from S3.
3. **Separation of concerns**: Ingestion (Bronze), quality (Silver), and business modeling (Gold) are independent steps with clear ownership boundaries.

### Dimensional Modeling Choices

**Star Schema over Snowflake Schema**: The Gold layer uses a flat star schema rather than a normalized snowflake schema. Dimensions like `dim_employee` embed foreign hash keys to `dim_department` and `dim_office` directly, rather than requiring multi-hop joins. This optimizes query performance on Snowflake's columnar engine.

**SCD Type-2 via Hash-Based Change Detection**: Rather than relying on source-system timestamps (which may be unreliable), the pipeline computes a SHA-256 hash of all tracked columns per business key. When a new delta arrives with a different hash, a new version row is created with `__EFFECTIVE_FROM_DATE` / `__EFFECTIVE_TO_DATE` windowing. This approach:
- Is deterministic and reproducible
- Does not depend on source timestamps being accurate
- Supports full rebuild from Bronze without data loss

**Surrogate Hash Keys**: All dimension and fact primary keys use `SHA2(CONCAT_WS('||', ...))` rather than identity columns. Benefits:
- Deterministic: same input always produces the same key (idempotent rebuilds)
- No sequence gaps or ordering dependency
- Works across full-refresh and incremental strategies without key conflicts

**Conformed Dimensions**: `dim_date`, `dim_proficiency`, and `dim_assignment_role` are conformed -- they are shared across multiple facts/bridges and use stable hash keys, enabling consistent cross-fact analysis.

### Delta Day Processing

Source data arrives as daily CSV delta files (day_01 through day_05) in addition to a base load. The pipeline:
1. Loads all files into Bronze with `METADATA$FILENAME` captured as `__STG_FILE_NAME`
2. Silver models extract the delta day from the filename via `REGEXP_SUBSTR(__STG_FILE_NAME, 'day_(\d+)', 1, 1, 'e')`
3. Gold SCD-2 models use `__SRC_DELTA_DAY` to order versions and compute effective dates

This means the pipeline supports both **full-refresh** (base + all deltas) and **incremental delta** (appending new day folders) workflows.

### Schema Separation Strategy

| Schema | Purpose | Materialization | Rationale |
|---|---|---|---|
| `BRONZE` | Raw landing zone | External load (COPY INTO) | Isolate raw data; append-only |
| `SILVER` | Cleansed intermediate | Transient tables | Save storage costs; rebuildable from Bronze |
| `GOLD` | Star schema + seeds | Persistent tables | Production-grade; supports Time Travel |
| `SNAPSHOTS` | dbt-managed SCD-2 | Snapshot tables | Separate from curated Gold to avoid confusion |
| `GOVERNANCE` | Tags for classification | Snowflake tags | Centralized governance metadata |
| `UTIL` | File formats, stages | Infrastructure objects | Utility objects shared across layers |

**Why Silver is transient**: Silver tables are fully deterministic from Bronze. Making them transient saves Snowflake storage costs (no Time Travel / Fail-safe overhead) since they can always be rebuilt.

**Why Snapshots are separate from Gold**: Snapshots are dbt-managed audit artifacts with `dbt_valid_from`/`dbt_valid_to` columns. They serve a different purpose than the modeled star schema. Keeping them in their own schema makes it clear what is curated output (Gold) vs. change-tracking history (Snapshots).

---

## Source Data (Bronze Layer)

Raw CSV files are staged in `DBT_HR_ANALYTICS.UTIL.MY_S3_STAGE` and loaded into `BRONZE` schema tables via macros. All columns are VARCHAR with two metadata columns appended:

- `__STG_FILE_NAME` — `METADATA$FILENAME` from the stage (used for delta day extraction)
- `__STG_LOAD_TS` — `METADATA$START_SCAN_TIME` (load audit timestamp)

| # | Bronze Table | Record Type | Description |
|---|---|---|---|
| 1 | DEPARTMENTS | Master | Department hierarchy (6 columns) |
| 2 | OFFICES | Master | Office locations with country/region (8 columns) |
| 3 | COMPANIES | Master | Client companies with industry/classification (8 columns) |
| 4 | EMPLOYEES | Master | Employee org-chart with job/manager data (14 columns) |
| 5 | PROJECTS | Master | Projects with budget/status/dates (14 columns) |
| 6 | EMPLOYEE_PROJECT_ASSIGNMENTS | Transaction | Staffing assignments with roles/allocation (7 columns) |
| 7 | EMPLOYEE_DAILY_ACCESS | Event | Badge in/out events per office (7 columns) |
| 8 | SKILLS | Master | Skill/technology catalog (6 columns) |
| 9 | EMPLOYEE_SKILLS | Bridge | Employee-skill proficiency mapping (5 columns) |
| 10 | PROJECT_TECHNOLOGIES | Bridge | Project technology requirements (5 columns) |

---

## Silver Layer

Each Silver model applies a consistent set of transformations:

1. **Type casting**: VARCHAR → NUMBER, DATE, BOOLEAN, TIMESTAMP_NTZ as appropriate
2. **Trimming**: `TRIM()` on all string columns to remove whitespace
3. **Null/empty filtering**: Records with null PKs or empty required fields are excluded
4. **Referential integrity**: Foreign keys are validated via `IN (SELECT pk FROM parent)` subqueries -- orphaned records are filtered out
5. **Enrichment**: Derived columns added where business logic dictates

| Model | Source | Key Transformations |
|---|---|---|
| `silver_departments` | DEPARTMENTS | Type casting, null/empty filtering |
| `silver_offices` | OFFICES | Country standardization via `country_mapping` seed (ISO codes, currency) |
| `silver_companies` | COMPANIES | Country standardization via `country_mapping` seed, delta day extraction |
| `silver_employees` | EMPLOYEES | Email normalization (`LOWER`), name standardization (`INITCAP`), email domain extraction |
| `silver_projects` | PROJECTS | Lifecycle state derivation (`Active`/`Overdue`/`Completed`/`Planning`), date sequence validation |
| `silver_employee_project_assignments` | EMPLOYEE_PROJECT_ASSIGNMENTS | Allocation fraction (÷100), active assignment flag, allocation range validation (0–100) |
| `silver_employee_daily_access` | EMPLOYEE_DAILY_ACCESS | Timestamp decomposition (date, time, hour), event type standardization, date consistency check |
| `silver_skills` | SKILLS | Skill code generation (`UPPER(REPLACE(name, ' ', '_'))`) |
| `silver_employee_skills` | EMPLOYEE_SKILLS | Proficiency rank derivation (Beginner=1 → Expert=4) |
| `silver_project_technologies` | PROJECT_TECHNOLOGIES | Required proficiency rank derivation |

### Silver Dependency Chain

```
bronze.DEPARTMENTS ──► silver_departments ──┐
bronze.OFFICES ─────► silver_offices ───────┤
                                            ├──► silver_employees
bronze.COMPANIES ───► silver_companies ─────┤
                                            ├──► silver_projects
bronze.SKILLS ──────► silver_skills ────────┤
                                            ├──► silver_employee_skills
                                            ├──► silver_project_technologies
                                            ├──► silver_employee_project_assignments
                                            └──► silver_employee_daily_access
```

---

## Gold Layer

Dimensional star schema with SCD Type-2 history tracked via SHA-256 hash-based change detection.

### Dimensions

| Model | SCD Type | Business Key | Tracked Columns | Description |
|---|---|---|---|---|
| `dim_date` | Generated | DATE_DAY | N/A | Calendar dimension spanning 2024-01-01 to 2034-01-01 (3,660 rows). Includes day-of-week, month, quarter, year, and business day flag. |
| `dim_department` | SCD-2 | DEPARTMENT_ID | CODE, NAME, IS_ACTIVE | Department attributes with version history |
| `dim_office` | SCD-2 | OFFICE_ID | CODE, CITY, COUNTRY, REGION, IS_ACTIVE | Office location attributes with version history |
| `dim_company` | SCD-2 | COMPANY_ID | NAME, INDUSTRY, COUNTRY, CLASSIFICATION, IS_ACTIVE | Client company attributes with version history |
| `dim_employee` | SCD-2 | EMPLOYEE_ID | ACCESS_ID, NAME, EMAIL, DEPT, OFFICE, MANAGER, TITLE, LEVEL, STATUS, HIRE_DATE, IS_ACTIVE | Employee attributes with FK hash keys to dim_department and dim_office |
| `dim_project` | SCD-2 | PROJECT_ID | NAME, COMPANY, DEPT, TYPE, BILLING, STATUS, DATES, IS_ACTIVE | Project attributes with FK hash keys to dim_company and dim_department |
| `dim_skill` | Type-1 | SKILL_ID | N/A (latest only) | Latest skill record per SKILL_ID via ROW_NUMBER dedup |
| `dim_proficiency` | Reference | PROFICIENCY_LEVEL | N/A (static seed) | 4 proficiency bands: Beginner (1), Intermediate (2), Advanced (3), Expert (4) |
| `dim_assignment_role` | Reference | ASSIGNMENT_ROLE | N/A (distinct values) | Distinct roles extracted from assignment data |

### Facts & Bridges

| Model | Grain | Surrogate Key | FK Dimensions | Description |
|---|---|---|---|---|
| `fact_employee_daily_access` | One row per badge event | ACCESS_EVENT_HK | dim_employee, dim_office | Badge in/out events resolved to employee and office hash keys |
| `fact_employee_project_assignment` | One row per assignment version | ASSIGNMENT_VERSION_HK | dim_employee, dim_project, dim_assignment_role | Staffing assignments with SCD-2 versioning, allocation %, and date range |
| `fct_project_budget_plan` | One row per budget version | PROJECT_BUDGET_VERSION_HK | dim_project, dim_company, dim_department | Project budget changes tracked as version history |
| `br_employee_skill` | One row per skill version | EMPLOYEE_SKILL_VERSION_HK | dim_employee, dim_skill, dim_proficiency | Employee-skill proficiency bridge with effective dating |
| `br_project_skill_requirement` | One row per requirement version | PROJECT_TECHNOLOGY_VERSION_HK | dim_project, dim_skill, dim_proficiency | Project technology requirement bridge with effective dating |

### SCD-2 Column Conventions

All SCD-2 models follow a consistent naming pattern:

| Column | Purpose |
|---|---|
| `*_HK` | SHA-256 surrogate hash key (primary key) |
| `__IS_CURRENT` | `TRUE` for the latest version of each business key |
| `__ROW_HASH` | SHA-256 hash of all tracked columns (used for change detection) |
| `__EFFECTIVE_FROM_DATE` | Version start date (derived from delta day) |
| `__EFFECTIVE_TO_DATE` | Version end date (`9999-12-31` for current) |

### Gold Dependency Chain

```
silver_departments ──► dim_department ──────────┐
silver_offices ─────► dim_office ───────────────┤
silver_companies ───► dim_company ──────────────┤
                                                ├──► dim_employee ──────┐
                                                ├──► dim_project ───────┤
silver_skills ──────► dim_skill                 │                       │
seed: proficiency ──► dim_proficiency           │                       │
                                                │                       │
silver_emp_proj_assignments ────────────────────┼──► fact_employee_project_assignment
silver_emp_daily_access ────────────────────────┼──► fact_employee_daily_access
silver_projects ────────────────────────────────┼──► fct_project_budget_plan
silver_employee_skills ─────────────────────────┼──► br_employee_skill
silver_project_technologies ────────────────────┼──► br_project_skill_requirement
silver_emp_proj_assignments ────────────────────└──► dim_assignment_role
```

---

## Seeds

Seeds are version-controlled CSV files loaded into the Gold schema as reference tables.

| Seed | Target Schema | Rows | Purpose | Consumed By |
|---|---|---|---|---|
| `country_mapping` | GOLD | 26 | Maps raw country strings (e.g., "US", "USA", "United States") to ISO-2/ISO-3 codes, standardized names, and default currency codes | `silver_offices`, `silver_companies` |
| `proficiency_levels` | GOLD | 4 | Static reference of proficiency bands with ordinal rank (Beginner=1 through Expert=4) | `dim_proficiency` |

### Why Seeds over Hardcoded CTEs?

Country mapping was originally hardcoded as a `VALUES` CTE in both `silver_offices` and `silver_companies`. Moving it to a seed provides:
- **Single source of truth**: one CSV file, not duplicated SQL
- **Versioned in git**: changes are tracked and reviewable
- **Testable**: YAML tests validate uniqueness and not-null constraints
- **Easy to extend**: adding a new country is a CSV edit, not a SQL change in multiple files

---

## Snapshots

Snapshots live in a dedicated `SNAPSHOTS` schema, separate from the curated Gold layer.

| Snapshot | Target Schema | Strategy | Unique Key | Check Columns | Source | Description |
|---|---|---|---|---|---|---|
| `snap_skills` | SNAPSHOTS | `check` | SKILL_ID | SKILL_NAME, SKILL_CATEGORY, IS_ACTIVE | `silver_skills` | Tracks skill attribute changes over time with dbt-managed `dbt_valid_from` / `dbt_valid_to` timestamps |

### Why a Separate Snapshots Schema?

`dim_skill` in Gold is a Type-1 dimension (latest record only). `snap_skills` provides complementary SCD-2 history managed by dbt's snapshot mechanism. Keeping snapshots separate from Gold:
- Avoids mixing curated star-schema output with audit artifacts
- Makes the Gold schema a clean, well-defined contract for downstream consumers
- Allows different access controls if needed

---

## Macros

| Macro | File | Purpose |
|---|---|---|
| `hash_key(columns)` | `macros/hash_key.sql` | Generates deterministic SHA-256 surrogate keys using `CONCAT_WS('||', COALESCE(CAST(col AS VARCHAR), '^^NULL^^'), ...)`. Null-safe via sentinel value to distinguish `NULL || 'A'` from `'A' || NULL`. |
| `generate_schema_name` | `macros/generate_schema_name.sql` | Overrides dbt's default schema naming to use the custom schema directly (e.g., `SILVER`, `GOLD`) rather than prefixing with the target schema. |
| `setup_infrastructure` | `macros/setup_infrastructure.sql` | Creates the CSV file format (`CSV_FF`), external S3 stage (`MY_S3_STAGE`), and 4 governance tags in a single idempotent operation. |
| `load_bronze` | `macros/load_bronze.sql` | Loads base CSV files from stage into Bronze tables. Creates tables if they don't exist. Supports optional `day_folder` argument for targeted delta loads. Captures `METADATA$FILENAME` and `METADATA$START_SCAN_TIME`. |
| `load_bronze_deltas` | `macros/load_bronze_deltas.sql` | Iterates through daily-incremental delta folders (day_01 through day_05) and loads each into the corresponding Bronze table. |
| `create_semantic_view` | `macros/create_semantic_view.sql` | Creates the `HR_ANALYTICS_MODEL` Snowflake Semantic View over the full Gold star schema with annotated relationships, facts, and dimensions. |

---

## Testing Strategy

The project employs a **two-tier testing approach**: generic YAML-based tests and custom singular SQL tests.

### Generic Tests (68 tests, defined in YAML)

Declared in `_sources.yml`, `_silver__models.yml`, `_gold__models.yml`, and `_seeds_models.yml`:

| Test Type | Count | Applied To | Purpose |
|---|---|---|---|
| `not_null` | ~30 | PKs across all layers | Ensures no null primary/business keys |
| `unique` | ~15 | PKs and hash keys | Ensures key uniqueness within tables |
| `relationships` | 5 | Silver FKs → Silver parents | Validates referential integrity (e.g., employee.DEPARTMENT_ID → department.DEPARTMENT_ID) |
| `accepted_values` | 1 | dim_proficiency.PROFICIENCY_LEVEL | Ensures only valid proficiency bands exist |

### Custom Singular Tests (4 tests, in `tests/` folder)

Cross-table business rule validations that cannot be expressed as generic YAML tests:

| Test File | Severity | What It Checks | Current Result |
|---|---|---|---|
| `assert_no_assignment_date_violations.sql` | error | Assignment end dates must not precede start dates | PASS |
| `assert_no_orphaned_access_events.sql` | error | Every badge event must resolve to a known employee | PASS |
| `assert_no_budget_without_assignments.sql` | error | Active projects with budget must have at least one staffing assignment | PASS |
| `assert_no_employee_allocation_overcommit.sql` | warn | Flags employees whose current total allocation exceeds 100% | WARN (34 employees) |

### How Custom Tests Work

Any `.sql` file in `tests/` is a SELECT query that returns **rows that fail** the assertion. Zero rows = PASS. Any rows returned = FAIL (or WARN if configured with `severity='warn'`).

The allocation overcommit test is configured as `warn` rather than `error` because overlapping assignments during role transitions are a legitimate business scenario in HR data.

---

## Semantic View

The `create_semantic_view` macro creates `DBT_HR_ANALYTICS.GOLD.HR_ANALYTICS_MODEL`, a Snowflake Semantic View that exposes the full Gold star schema for use with Cortex Analyst.

The semantic view defines:
- **14 tables** with primary keys and descriptive comments
- **30 relationships** mapping all foreign key paths between facts, bridges, and dimensions
- **1 fact measure**: `PROJECT_BUDGET_USD`
- **80+ dimension attributes** with business-friendly comments

This enables natural-language querying over the HR analytics star schema without requiring users to understand the underlying join paths.

---

## Data Governance

The `setup_infrastructure` macro creates four governance tags in `DBT_HR_ANALYTICS.GOVERNANCE`:

| Tag | Allowed Values | Applied To | Purpose |
|---|---|---|---|
| `ENV_TAG` | DEV, QA, PROD | Database/schema level | Environment classification |
| `DATA_CLASSIFICATION_TAG` | PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED | Column level | Data sensitivity classification |
| `PII_TAG` | NAME, EMAIL, NONE | Column level | PII sub-classification for compliance |
| `DATA_DOMAIN_TAG` | HR, PROJECT, ACCESS, FINANCE | Table level | Business domain ownership |

These tags are created but not yet applied to objects. Apply them using:

```sql
ALTER TABLE DBT_HR_ANALYTICS.GOLD.DIM_EMPLOYEE
    SET TAG DBT_HR_ANALYTICS.GOVERNANCE.DATA_DOMAIN_TAG = 'HR';

ALTER TABLE DBT_HR_ANALYTICS.GOLD.DIM_EMPLOYEE
    MODIFY COLUMN EMPLOYEE_EMAIL
    SET TAG DBT_HR_ANALYTICS.GOVERNANCE.PII_TAG = 'EMAIL';
```

---

## How to Run

### Full Pipeline (first time)

```bash
# 1. Set up infrastructure (file format, stage, governance tags)
dbt run-operation setup_infrastructure --project-dir dbt-snowflake-project

# 2. Load base data into Bronze
dbt run-operation load_bronze --project-dir dbt-snowflake-project

# 3. Load daily incremental deltas into Bronze
dbt run-operation load_bronze_deltas --project-dir dbt-snowflake-project

# 4. Full build: seeds + models + snapshot + tests (in dependency order)
dbt build --project-dir dbt-snowflake-project

# 5. Create semantic view (optional, for Cortex Analyst)
dbt run-operation create_semantic_view --project-dir dbt-snowflake-project
```

### Incremental Delta Load (subsequent runs)

```bash
# Load a specific day's delta
dbt run-operation load_bronze --args '{day_folder: day_06}' --project-dir dbt-snowflake-project

# Rebuild Silver + Gold with new data
dbt build --project-dir dbt-snowflake-project
```

### Individual Operations

```bash
# Seeds only
dbt seed --project-dir dbt-snowflake-project

# Models only (no tests)
dbt run --project-dir dbt-snowflake-project

# Snapshot only
dbt snapshot --project-dir dbt-snowflake-project

# Tests only
dbt test --project-dir dbt-snowflake-project

# Specific model and its downstream
dbt build --select silver_employees+ --project-dir dbt-snowflake-project
```

---

## Project Configuration

| Setting | Value |
|---|---|
| **Project name** | `dbt_hr_analytics` |
| **dbt version** | 1.9.4 |
| **Snowflake adapter** | 1.9.2 |
| **Database** | `DBT_HR_ANALYTICS` |
| **Profile** | `dbt_hr_analytics` |
| **Threads** | 8 |
| **Role** | `SYSADMIN` |
| **Warehouse** | `SANDBOX_WH` |

| Schema | Purpose | Materialization |
|---|---|---|
| `UTIL` | File formats, stages | Infrastructure objects |
| `BRONZE` | Raw landing zone | COPY INTO (external load) |
| `SILVER` | Cleansed models | Transient tables |
| `GOLD` | Star schema + seeds | Persistent tables |
| `SNAPSHOTS` | dbt snapshots | Snapshot tables |
| `GOVERNANCE` | Classification tags | Snowflake tags |

---

## DAG Overview

Total objects managed by dbt: **24 models, 2 seeds, 1 snapshot, 72 tests**

```
Sources (10 Bronze tables)
    │
    ├── Silver (10 models)
    │       │
    │       ├── Gold Dimensions (9 models)
    │       │       │
    │       │       ├── Gold Facts (3 models)
    │       │       └── Gold Bridges (2 models)
    │       │
    │       └── Snapshots (1 snapshot → SNAPSHOTS schema)
    │
    └── Seeds (2 CSVs → GOLD schema)
            │
            └── Gold Dimensions (dim_proficiency)

Tests: 68 generic (YAML) + 4 custom (SQL) = 72 total
```

Latest build: **99/99 — PASS=98, WARN=1, ERROR=0**
