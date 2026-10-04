# dbt on Snowflake — HR Analytics

## Legacy ETL Setup

Run the SQL files in `legacy-etl-setup/` in order:

1. `01-hr-analytics-deploy.sql` — Creates the database, schemas, tables, stored procedures, streams, and tasks.
2. `02-put_full_load.sql` — Loads base CSV files into the Bronze layer and runs the Silver and Gold pipelines.
3. `03-put_delta_load.sql` — Loads daily delta CSV files and processes them through the pipeline.
4. `04-tear-down.sql` — Drops all objects created by the setup.
