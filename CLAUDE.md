# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start: Complete Environment Setup

### Step 1: Start Astro
```bash
astro dev start
```

### Step 2: Set Up Databases

**Choose your target database type (Postgres OR SQL Server):**

#### Option A: PostgreSQL Target (Simpler)
```bash
# Start Postgres source
docker run -d --name pagila-pg-source \
  -e POSTGRES_DB=pagila -e POSTGRES_USER=pagila -e POSTGRES_PASSWORD=pagila_pw \
  -p 5433:5432 postgres:16

# Start Postgres target
docker run -d --name pagila-pg-target \
  -e POSTGRES_DB=pagila -e POSTGRES_USER=pagila_tgt -e POSTGRES_PASSWORD=pagila_tgt_pw \
  -p 5444:5432 postgres:16

# Connect to Astro network
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'pagila-demo-project.*_airflow')
docker network connect $ASTRO_NETWORK pagila-pg-source
docker network connect $ASTRO_NETWORK pagila-pg-target
```

#### Option B: SQL Server Target (Cross-Database Replication)
```bash
# Start Postgres source
docker run -d --name pagila-pg-source \
  -e POSTGRES_DB=pagila -e POSTGRES_USER=pagila -e POSTGRES_PASSWORD=pagila_pw \
  -p 5433:5432 postgres:16

# Start SQL Server target (Azure SQL Edge for ARM64 compatibility)
docker run -d --name pagila-mssql-target \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=PagilaPass123!" \
  -p 1433:1433 mcr.microsoft.com/azure-sql-edge:latest

# Connect to Astro network
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'pagila-demo-project.*_airflow')
docker network connect $ASTRO_NETWORK pagila-pg-source
docker network connect $ASTRO_NETWORK pagila-mssql-target
```

### Step 3: Create Airflow Connections

**Source connection (always required):**
```bash
astro dev run connections add pagila_postgres \
  --conn-type postgres --conn-host pagila-pg-source --conn-port 5432 \
  --conn-login pagila --conn-password pagila_pw --conn-schema pagila
```

**Target connection (choose based on Step 2):**
```bash
# For Postgres target
astro dev run connections add pagila_tgt \
  --conn-type postgres --conn-host pagila-pg-target --conn-port 5432 \
  --conn-login pagila_tgt --conn-password pagila_tgt_pw --conn-schema pagila

# OR for SQL Server target
astro dev run connections add pagila_mssql \
  --conn-type mssql --conn-host pagila-mssql-target --conn-port 1433 \
  --conn-login sa --conn-password "PagilaPass123!" --conn-schema master
```

### Step 4: Run DAGs
```bash
# Load source data
astro dev run dags unpause load_pagila_to_postgres
astro dev run dags trigger load_pagila_to_postgres

# Wait for source load to complete, then replicate to target
astro dev run dags unpause replicate_pagila_to_target
astro dev run dags trigger replicate_pagila_to_target
```

### Step 5: Verify
```bash
# For Postgres target
docker exec -it pagila-pg-target psql -U pagila_tgt -d pagila -c "SELECT COUNT(*) FROM rental;"

# For SQL Server target
docker exec -it pagila-mssql-target /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "PagilaPass123!" -Q "USE pagila_target; SELECT COUNT(*) FROM dbo.rental;"
```
Expected: 16,044 rows in rental table

---

## Project Overview

This is a **Pagila End-to-End Data Replication Pipeline** using Apache Airflow 3 and PostgreSQL/SQL Server. It demonstrates production-quality data engineering practices including idempotent data loading, memory-capped streaming replication, audit logging, and resource management.

**Key Components:**
- Source PostgreSQL database (port 5433) with Pagila sample data (DVD rental store)
- Target database: PostgreSQL (port 5444) OR SQL Server (port 1433)
- Apache Airflow DAGs for orchestration
- Audit trail and data validation
- Cross-database replication with type conversion support

## Development Commands

### Environment Management
```bash
astro dev start          # Start local Airflow environment
astro dev restart        # Reload after dependency changes (requirements.txt, DAG code)
astro dev stop           # Stop all containers
```

### Testing
```bash
astro dev run pytest tests/dags                    # Run DAG validation tests
astro dev run dags test load_pagila_to_postgres <date>  # Dry-run specific DAG
```

### DAG Operations
```bash
astro dev run dags unpause load_pagila_to_postgres     # Enable DAG scheduling
astro dev run dags trigger load_pagila_to_postgres     # Manual trigger
astro dev run dags trigger replicate_pagila_to_target  # Trigger replication
astro dev run connections list                         # View Airflow connections
```

### Database Setup

#### PostgreSQL Source (Always Required)
```bash
# Start source database
docker run -d --name pagila-pg-source \
  -e POSTGRES_DB=pagila -e POSTGRES_USER=pagila -e POSTGRES_PASSWORD=pagila_pw \
  -p 5433:5432 postgres:16
```

#### Target Database (Choose One)

**Option A: PostgreSQL Target**
```bash
# Start Postgres target database  
docker run -d --name pagila-pg-target \
  -e POSTGRES_DB=pagila -e POSTGRES_USER=pagila_tgt -e POSTGRES_PASSWORD=pagila_tgt_pw \
  -p 5444:5432 postgres:16
```

**Option B: SQL Server Target**
```bash
# Start SQL Server target (Azure SQL Edge for ARM64/AMD64 compatibility)
docker run -d --name pagila-mssql-target \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=PagilaPass123!" \
  -p 1433:1433 mcr.microsoft.com/azure-sql-edge:latest
```

#### Network Connection
```bash
# Find Astro network
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'pagila-demo-project.*_airflow')

# Connect source (always required)
docker network connect $ASTRO_NETWORK pagila-pg-source

# Connect target (choose one based on your setup)
docker network connect $ASTRO_NETWORK pagila-pg-target      # for Postgres target
docker network connect $ASTRO_NETWORK pagila-mssql-target   # for SQL Server target
```

### Download SQL Files
```bash
# Postgres schema and data (required for source, Postgres target)
curl -fsSL https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql -o include/pagila/schema.sql
curl -fsSL https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql -o include/pagila/data.sql

# Note: schema_mssql.sql is already in the repository for SQL Server target
```

## Architecture Overview

### DAGs Structure
1. **`load_pagila_dag.py`** - Idempotent source data loading
   - Uses SHA256 hashing to prevent duplicate loads
   - Records audit trail in `pagila_support.pagila_load_audit`
   - Resets `public` schema and loads fresh data
   - Stream processes COPY blocks from SQL files

2. **`replicate_pagila_to_target.py`** - Memory-capped streaming replication
   - **Postgres Target**: Uses `SpooledTemporaryFile` with direct COPY TO/FROM streaming
   - **SQL Server Target**: Uses CSV-based bulk loading with `io.TextIOWrapper` and batch INSERT
   - Memory limit: 128MB in-memory buffer before disk spill
   - Copies tables in dependency order to maintain referential integrity
   - Fixes auto-increment sequences after copy (setval for Postgres, DBCC CHECKIDENT for SQL Server)
   - Logs memory usage and disk spill events

### Target Database Support
The replication DAG supports both **PostgreSQL** and **SQL Server** targets:

| Feature | Postgres Target | SQL Server Target |
|---------|----------------|-------------------|
| Hook | `PostgresHook` | `MsSqlHook` |
| Connection ID | `pagila_tgt` | `pagila_mssql` |
| Schema File | `schema.sql` | `schema_mssql.sql` |
| Data Transfer | COPY TO/FROM binary | CSV + batch INSERT |
| Truncate | `TRUNCATE TABLE` | `DELETE FROM` |
| Sequences | `setval()` | `DBCC CHECKIDENT` |
| Required Packages | psycopg2-binary | pymssql, apache-airflow-providers-microsoft-mssql |

### Table Dependency Order
```
language → category → actor → film → film_actor → film_category → 
country → city → address → store → customer → staff → 
inventory → rental → payment
```

### Key Configuration
- **Resource Allocation**: Scheduler (2Gi), Webserver (1Gi), Triggerer (512Mi)
- **Parallelism**: 16 parallel tasks, 8 active tasks per DAG, 2 active runs per DAG
- **Memory Streaming**: 128MB in-memory buffer before disk spill

## Code Conventions

### DAG Requirements (Enforced by Tests)
- All DAGs must have tags
- All DAGs must have `retries >= 2` in `default_args`
- No import errors allowed

### Connection IDs
- `pagila_postgres` - Source database connection (always Postgres)
- `pagila_tgt` - Postgres target database connection
- `pagila_mssql` - SQL Server target database connection

### File Paths
- SQL files (Postgres): `/usr/local/airflow/include/pagila/schema.sql`, `/usr/local/airflow/include/pagila/data.sql`
- SQL files (SQL Server): `/usr/local/airflow/include/pagila/schema_mssql.sql`
- Use `template_searchpath=["/usr/local/airflow/include"]` in DAG definition

### Python Style
- Python 3.10+ with type hints
- PEP 8 spacing (4-space indents)
- `snake_case` for DAG IDs and task IDs
- Module-level uppercase constants for shared values

## Technology Stack

- **Apache Airflow 3** on Astro Runtime 3.1-3
- **PostgreSQL 16** for source and optional target databases
- **SQL Server** (Azure SQL Edge) for optional target database
- **Python 3.10+** with psycopg2-binary (Postgres) or pymssql (SQL Server)
- **Docker** for database containerization
- **pytest** for DAG validation

## SQL Server Type Conversions

When using SQL Server as the target, the following type conversions are applied:

| PostgreSQL Type | SQL Server Type | Notes |
|----------------|----------------|-------|
| `SERIAL` / `BIGSERIAL` | `INT IDENTITY(1,1)` | Auto-increment primary keys |
| `TEXT` | `NVARCHAR(MAX)` | Unicode support |
| `VARCHAR(n)` | `NVARCHAR(n)` | Unicode support |
| `TIMESTAMP` | `DATETIME2` | Higher precision |
| `BOOLEAN` | `BIT` | True/False → 1/0 |
| `BYTEA` | `VARBINARY(MAX)` | Binary data |
| `NUMERIC` | `DECIMAL` | Exact numeric |
| `NOW()` | `GETDATE()` | Current timestamp |

**Removed PostgreSQL-specific features:**
- `DOMAIN` types (converted to base types with constraints)
- `ENUM` types (converted to `NVARCHAR` with check constraints)
- `pl/pgsql` functions and triggers
- `GO` batch separators (not supported by pymssql)

See `MSSQL_MIGRATION.md` for complete conversion details and troubleshooting.

## Key Files

- `dags/load_pagila_dag.py` - Source data loading with idempotency
- `dags/replicate_pagila_to_target.py` - Streaming replication logic (supports Postgres and SQL Server)
- `include/pagila/schema.sql` - Pagila database schema (Postgres)
- `include/pagila/schema_mssql.sql` - Pagila database schema (SQL Server T-SQL)
- `include/pagila/data.sql` - Pagila sample data
- `tests/dags/test_dag_example.py` - DAG validation tests
- `.astro/config.yaml` - Resource allocation settings
- `Dockerfile` - Airflow parallelism configuration
- `MSSQL_MIGRATION.md` - SQL Server migration guide and troubleshooting
- `AGENTS.md` - Repository guidelines for AI agents and contributors

## Verification Commands

### PostgreSQL Target
```bash
# Check row counts on Postgres target
docker exec -it pagila-pg-target psql -U pagila_tgt -d pagila -c "SELECT COUNT(*) FROM actor;"

# View audit history on source
docker exec -it pagila-pg-source psql -U pagila -d pagila -c "SELECT loaded_at, schema_sha256, data_sha256, succeeded FROM pagila_support.pagila_load_audit ORDER BY loaded_at DESC LIMIT 5;"
```

### SQL Server Target
```bash
# Check row counts on SQL Server target
docker exec -it pagila-mssql-target /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "PagilaPass123!" -Q "USE pagila_target; SELECT 'actor' AS table_name, COUNT(*) AS row_count FROM dbo.actor;"

# Check all table counts
docker exec -it pagila-mssql-target /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "PagilaPass123!" -Q "USE pagila_target; SELECT 'language' AS t, COUNT(*) AS c FROM dbo.language UNION ALL SELECT 'rental', COUNT(*) FROM dbo.rental UNION ALL SELECT 'payment', COUNT(*) FROM dbo.payment;"
```

### Expected Row Counts (All Targets)
- language: 6
- category: 16
- actor: 200
- film: 1,000
- film_actor: 5,462
- film_category: 1,000
- country: 109
- city: 600
- address: 605
- store: 2
- customer: 599
- staff: 2
- inventory: 4,581
- rental: 16,044
- payment: 16,049

## SQL Server Replication Status

✅ **Fully Tested and Working** (as of 2024-12-08)

The SQL Server target replication has been extensively tested and successfully replicates all 15 tables from PostgreSQL source to Azure SQL Edge target.

**Test Results:**
- **DAG Status**: SUCCESS
- **Runtime**: ~29 seconds
- **Tables Replicated**: 15/15 (100%)
- **Total Rows**: 45,000+
- **Data Validation**: All row counts match expected values
- **Identity Sequences**: Successfully aligned

**All 14 Migration Challenges Solved:**
1. ✅ IDENTITY_INSERT handling
2. ✅ Datetime timezone conversion
3. ✅ Column count matching
4. ✅ Conditional IDENTITY_INSERT detection
5. ✅ Empty string vs NULL handling
6. ✅ Boolean conversion (t/f → 1/0)
7. ✅ Circular FK dependencies (store ↔ staff)
8. ✅ VARCHAR size adjustments
9. ✅ Schema differences (payment.last_update, film.fulltext)
10. ✅ GO batch separator removal
11. ✅ Autocommit API compatibility
12. ✅ Binary vs text mode handling
13. ✅ TRUNCATE to DELETE conversion
14. ✅ Database connection and creation

For detailed troubleshooting and all solutions, see `MSSQL_MIGRATION.md`.