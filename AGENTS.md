<!-- Git commit message: Remove advisory lock tasks and enforce shared default retries -->
# Repository Guidelines

## Project Structure & Module Organization
- `dags/` holds the production DAGs; `load_pagila_dag.py` bootstraps the source schema and `replicate_pagila_to_target.py` streams data into the target.
- `include/pagila/` stores the canonical SQL assets:
  - `schema.sql` - Postgres DDL for source and Postgres target
  - `data.sql` - Postgres COPY data for source initialization
  - `schema_mssql.sql` - T-SQL DDL for SQL Server target (converted from Postgres schema)
- `plugins/` is reserved for custom Airflow components; keep it empty unless you add operators or hooks tested against Astro.
- `tests/dags/` contains pytest suites that exercise import hygiene, tagging, and retry defaults for every DAG.
- `MSSQL_MIGRATION.md` documents the SQL Server target implementation, type conversions, and troubleshooting.

## Build, Test, and Development Commands
- `astro dev start` spins up the local Airflow environment; run it from the repo root after Docker and Astro prerequisites are met.
- `astro dev restart` reloads dependencies when `requirements.txt` or DAG code changes.
- `astro dev run pytest tests/dags` executes the DAG-focused pytest suite inside the containerized runtime.
- `astro dev run dags test replicate_pagila_to_target <iso8601>` dry-runs a specific DAG; substitute the execution date you want to validate.

### Database Container Setup
**Postgres Source** (always required):
```bash
docker run -d --name pagila-pg-source \
  -e POSTGRES_DB=pagila -e POSTGRES_USER=pagila -e POSTGRES_PASSWORD=pagila_pw \
  -p 5433:5432 postgres:16
```

**Postgres Target** (option A):
```bash
docker run -d --name pagila-pg-target \
  -e POSTGRES_DB=pagila -e POSTGRES_USER=pagila_tgt -e POSTGRES_PASSWORD=pagila_tgt_pw \
  -p 5444:5432 postgres:16
```

**SQL Server Target** (option B):
```bash
docker run -d --name pagila-mssql-target \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=PagilaPass123!" \
  -p 1433:1433 mcr.microsoft.com/azure-sql-edge:latest
```

**Network Connection**:
```bash
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'pagila-demo-project.*_airflow')
docker network connect $ASTRO_NETWORK pagila-pg-source
docker network connect $ASTRO_NETWORK pagila-pg-target  # or pagila-mssql-target
```

## Coding Style & Naming Conventions
- Use Python 3.10+ with PEP 8 spacing (four-space indents) and type hints, mirroring existing DAG modules.
- Name DAG ids and task ids in `snake_case`, matching the Airflow conventions already in `replicate_pagila_to_target`.
- Store shared constants (connection ids, table lists) in uppercase module-level variables so the hooks remain easy to audit.
- Keep airflow-specific SQL templates in `include/` and reference them with relative paths (`pagila/schema.sql`) via `template_searchpath`.

## Testing Guidelines
- Tests use `pytest` plus Airflow’s `DagBag`; failures usually mean import errors, missing tags, or retries under two.
- Add new DAG tests under `tests/dags/` and mirror the parametrized patterns in `test_dag_example.py`.
- Name fixtures and test functions descriptively (`test_copy_task_idempotent`) so pytest output maps to specific DAG behaviors.
- Run tests inside Astro with `astro dev run pytest` to ensure plugins and Airflow configs load exactly as they do in production.

## Commit & Pull Request Guidelines
- Follow the repo's history: start commit subjects with an imperative verb and keep them under ~72 characters (`Enhance audit logging...`).
- Group related Astro, SQL, and documentation updates into a single commit to keep DAG revisions traceable.
- Open pull requests with: scope summary, key validation notes (e.g., `astro dev run pytest` output), linked tickets, and screenshots/log excerpts for UI or data-impacting changes.
- Highlight any new connections, environment variables, or credentials required so reviewers can reproduce the pipeline locally.

## SQL Server Target Notes
- The DAG supports both Postgres and SQL Server targets via different connection IDs (`pagila_tgt` vs `pagila_mssql`).
- SQL Server schema lives in `include/pagila/schema_mssql.sql` with T-SQL syntax and converted data types.
- Key conversions: `SERIAL`→`INT IDENTITY`, `TEXT`→`NVARCHAR(MAX)`, `TIMESTAMP`→`DATETIME2`, `BOOLEAN`→`BIT`.
- Data flow: Postgres COPY CSV → SpooledTemporaryFile (128MB cap) → io.TextIOWrapper → SQL Server batch INSERT.
- **Data transformations applied**:
  - Datetime: Strip timezone info (`+00`, `Z`) for DATETIME2 compatibility
  - Boolean: Convert `t`/`f` to `1`/`0` for BIT columns
  - Empty strings: Keep as `""` for NOT NULL columns, convert to NULL for nullable columns
  - Column matching: Query target schema, only copy matching columns from source
- **FK constraint handling**: Disable with `NOCHECK CONSTRAINT ALL` during load, re-enable with `CHECK CONSTRAINT ALL`
- **IDENTITY_INSERT**: Query `sys.identity_columns`, only enable for tables with identity property
- Use `DELETE FROM` instead of `TRUNCATE` due to foreign key constraints.
- Use `DBCC CHECKIDENT` instead of `setval()` for identity sequence reseeding.
- pymssql requires `conn.autocommit(True)` method call, not attribute assignment.
- **Schema differences from Postgres**:
  - `username` column: `NVARCHAR(50)` instead of `NVARCHAR(16)` to accommodate longer usernames
  - `payment` table: No `last_update` column (Postgres version is partitioned)
  - `film` table: No `fulltext` column (tsvector type has no direct SQL Server equivalent)
- See `MSSQL_MIGRATION.md` for complete migration documentation, all 14 challenges solved, and troubleshooting.
