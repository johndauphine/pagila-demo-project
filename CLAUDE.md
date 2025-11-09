# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Database Platform Notes

**SQL Server 2022** is the recommended source database platform:
- Production-grade stability and performance
- Full compatibility with StackOverflow2010 database
- Works on AMD64/x86_64 architecture (required for SQL Server 2022)
- 4GB RAM allocation recommended for large datasets

**ARM64 (Apple Silicon) Compatibility:**
- SQL Server 2022 does NOT support ARM64/Apple Silicon
- Use AMD64/x86_64 hardware or cloud VMs (AWS, Azure, GCP)
- Alternative: Use PostgreSQL as both source and target on ARM64

**PostgreSQL Target Benefits:**
- Cross-platform compatibility (works on ARM64)
- No licensing costs
- Excellent performance for data warehousing workloads
- Better stability on non-Windows platforms

---

## Quick Start: Complete Environment Setup

### Step 1: Start Astro
```bash
astro dev start
```

### Step 2: Set Up SQL Server Databases

**SQL Server-to-SQL Server Replication Pipeline:**

```bash
# Start SQL Server 2022 source with StackOverflow2010 database
# Note: The .mdf and .ldf files are in include/stackoverflow/
# Requires AMD64/x86_64 architecture (SQL Server 2022 does not support ARM64)
docker run -d --name stackoverflow-mssql-source \
  --platform linux/amd64 \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -e "MSSQL_PID=Developer" \
  -v "$(pwd)/include/stackoverflow":/var/opt/mssql/backup \
  -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

# Start SQL Server 2022 target (4GB RAM for heavy write operations)
docker run -d --name stackoverflow-mssql-target \
  --platform linux/amd64 \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -e "MSSQL_PID=Developer" \
  -p 1434:1433 mcr.microsoft.com/mssql/server:2022-latest

# Connect to Astro network
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'stackoverflow.*_airflow')
docker network connect $ASTRO_NETWORK stackoverflow-mssql-source
docker network connect $ASTRO_NETWORK stackoverflow-mssql-target
```

### Step 3: Attach Source Database

```bash
# Copy database files into source container
docker exec stackoverflow-mssql-source mkdir -p /var/opt/mssql/data
docker cp include/stackoverflow/StackOverflow2010.mdf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2010_log.ldf stackoverflow-mssql-source:/var/opt/mssql/data/

# Fix file permissions (must run as root)
docker exec -u root stackoverflow-mssql-source chown mssql:mssql /var/opt/mssql/data/StackOverflow2010.mdf /var/opt/mssql/data/StackOverflow2010_log.ldf
docker exec -u root stackoverflow-mssql-source chmod 660 /var/opt/mssql/data/StackOverflow2010.mdf /var/opt/mssql/data/StackOverflow2010_log.ldf

# Attach the database (note: do NOT use quotes around password in this command)
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
  "CREATE DATABASE StackOverflow2010 ON (FILENAME = '/var/opt/mssql/data/StackOverflow2010.mdf'), (FILENAME = '/var/opt/mssql/data/StackOverflow2010_log.ldf') FOR ATTACH"
```

### Step 4: Create Airflow Connections

```bash
# Source connection
astro dev run connections add stackoverflow_source \
  --conn-type mssql --conn-host stackoverflow-mssql-source --conn-port 1433 \
  --conn-login sa --conn-password "StackOverflow123!" --conn-schema StackOverflow2010

# Target connection
astro dev run connections add stackoverflow_target \
  --conn-type mssql --conn-host stackoverflow-mssql-target --conn-port 1433 \
  --conn-login sa --conn-password "StackOverflow123!" --conn-schema master
```

### Step 5: Run DAGs

```bash
# Replicate Stack Overflow data from source to target
astro dev run dags unpause replicate_stackoverflow_to_target
astro dev run dags trigger replicate_stackoverflow_to_target
```

### Step 6: Verify

```bash
# Check row counts on target
docker exec stackoverflow-mssql-target /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q \
  "USE stackoverflow_target; SELECT 'Users' AS TableName, COUNT(*) AS RowCount FROM dbo.Users \
   UNION ALL SELECT 'Posts', COUNT(*) FROM dbo.Posts \
   UNION ALL SELECT 'Comments', COUNT(*) FROM dbo.Comments \
   UNION ALL SELECT 'Votes', COUNT(*) FROM dbo.Votes \
   UNION ALL SELECT 'Badges', COUNT(*) FROM dbo.Badges;"
```

Expected row counts (StackOverflow2010 database):
- Users: ~315,000
- Posts: ~1.7 million
- Comments: ~1.3 million
- Votes: ~4.3 million
- Badges: ~190,000

---

## Alternative Setup: SQL Server to PostgreSQL Replication

> **⚠️ CRITICAL: macOS ARM64 (Apple Silicon) Limitation**
>
> **SQL Server 2022 does NOT natively support ARM64.** On M-series Macs:
> - SQL Server runs in **x86_64 emulation** (Rosetta 2)
> - **Performance**: Slower (emulation overhead)
> - **Stability**: Can crash during heavy operations
> - **Best for macOS ARM64**: Run SQL Server source on cloud AMD64 VM (AWS/Azure/GCP)
> - **Alternative**: Use PostgreSQL → PostgreSQL (fully native ARM64)
>
> For local testing on ARM64, expect slower performance and potential crashes with large datasets.

### Step 1: Start Astro
```bash
astro dev start
```

### Step 2: Set Up SQL Server Source and PostgreSQL Target

**SQL Server-to-PostgreSQL Replication Pipeline:**

```bash
# Start SQL Server 2022 source with StackOverflow2010 database
# Note: On ARM64 (Apple Silicon), this runs in x86_64 emulation mode (slower, less stable)
# Requires AMD64/x86_64 architecture OR ARM64 with emulation
docker run -d --name stackoverflow-mssql-source \
  --platform linux/amd64 \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -e "MSSQL_PID=Developer" \
  -v "$(pwd)/include/stackoverflow":/var/opt/mssql/backup \
  -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

# Start PostgreSQL 16 target (cross-platform, works on ARM64 and AMD64)
docker run -d --name stackoverflow-postgres-target \
  -e "POSTGRES_PASSWORD=StackOverflow123!" \
  -e "POSTGRES_USER=postgres" \
  -e "POSTGRES_DB=stackoverflow_target" \
  -p 5432:5432 postgres:16

# Connect to Astro network
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'stackoverflow.*_airflow')
docker network connect $ASTRO_NETWORK stackoverflow-mssql-source
docker network connect $ASTRO_NETWORK stackoverflow-postgres-target
```

### Step 3: Attach Source Database

```bash
# Copy database files into source container
docker exec stackoverflow-mssql-source mkdir -p /var/opt/mssql/data
docker cp include/stackoverflow/StackOverflow2010.mdf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2010_log.ldf stackoverflow-mssql-source:/var/opt/mssql/data/

# Fix file permissions (must run as root)
docker exec -u root stackoverflow-mssql-source chown mssql:mssql /var/opt/mssql/data/StackOverflow2010.mdf /var/opt/mssql/data/StackOverflow2010_log.ldf
docker exec -u root stackoverflow-mssql-source chmod 660 /var/opt/mssql/data/StackOverflow2010.mdf /var/opt/mssql/data/StackOverflow2010_log.ldf

# Attach the database (note: do NOT use quotes around password in this command)
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
  "CREATE DATABASE StackOverflow2010 ON (FILENAME = '/var/opt/mssql/data/StackOverflow2010.mdf'), (FILENAME = '/var/opt/mssql/data/StackOverflow2010_log.ldf') FOR ATTACH"
```

### Step 4: Create Airflow Connections

```bash
# Source connection (SQL Server)
astro dev run connections add stackoverflow_source \
  --conn-type mssql --conn-host stackoverflow-mssql-source --conn-port 1433 \
  --conn-login sa --conn-password "StackOverflow123!" --conn-schema StackOverflow2010

# Target connection (PostgreSQL)
astro dev run connections add stackoverflow_postgres_target \
  --conn-type postgres --conn-host stackoverflow-postgres-target --conn-port 5432 \
  --conn-login postgres --conn-password "StackOverflow123!" --conn-schema stackoverflow_target
```

### Step 5: Run DAGs

```bash
# Replicate Stack Overflow data from SQL Server to PostgreSQL
astro dev run dags unpause replicate_stackoverflow_to_postgres
astro dev run dags trigger replicate_stackoverflow_to_postgres
```

### Step 6: Verify

```bash
# Check row counts on PostgreSQL target
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT 'Users' AS tablename, COUNT(*) AS rowcount FROM \"Users\"
   UNION ALL SELECT 'Posts', COUNT(*) FROM \"Posts\"
   UNION ALL SELECT 'Comments', COUNT(*) FROM \"Comments\"
   UNION ALL SELECT 'Votes', COUNT(*) FROM \"Votes\"
   UNION ALL SELECT 'Badges', COUNT(*) FROM \"Badges\"
   ORDER BY tablename;"
```

Expected row counts (StackOverflow2010 database):
- Users: ~315,000
- Posts: ~1.7 million
- Comments: ~1.3 million
- Votes: ~4.3 million
- Badges: ~190,000

**Key Differences from SQL Server Target:**
- Uses PostgreSQL COPY command for faster bulk loading
- Automatic data type conversion (NVARCHAR → VARCHAR, DATETIME → TIMESTAMP, BIT → BOOLEAN)
- GENERATED ALWAYS AS IDENTITY instead of IDENTITY(1,1)
- Case-sensitive table/column names (quoted identifiers)
- No need for SET IDENTITY_INSERT equivalent
- Uses pg8000 driver (pure Python, fork-safe for LocalExecutor)

---

## Project Overview

This is a **Stack Overflow End-to-End Data Replication Pipeline** using Apache Airflow 3 with support for both SQL Server and PostgreSQL targets. It demonstrates production-quality data engineering practices including memory-capped streaming replication, audit logging, and resource management for large-scale datasets.

**Key Components:**
- Source SQL Server database (port 1433) with StackOverflow2010 sample data (Brent Ozar edition)
- Target databases: SQL Server (port 1434) or PostgreSQL (port 5432)
- Apache Airflow DAGs for orchestration
- Audit trail and data validation
- Memory-efficient streaming replication with 128MB buffer
- Cross-database data type mapping and conversion

**Stack Overflow Database:**
- Source: Brent Ozar's StackOverflow2010 database
- Size: 8.4GB (.mdf) + 256MB (.ldf)
- Data: 2008-2010 Stack Overflow posts, users, comments, votes, badges
- License: CC-BY-SA 3.0
- Download: https://downloads.brentozar.com/StackOverflow2010.7z

## Development Commands

### Environment Management
```bash
astro dev start          # Start local Airflow environment
astro dev restart        # Reload after dependency changes (requirements.txt, DAG code)
astro dev stop           # Stop all containers
```

### Testing
```bash
astro dev run pytest tests/dags                                      # Run DAG validation tests
astro dev run dags test replicate_stackoverflow_to_target <date>     # Dry-run SQL Server DAG
astro dev run dags test replicate_stackoverflow_to_postgres <date>   # Dry-run PostgreSQL DAG
```

### DAG Operations
```bash
# SQL Server to SQL Server
astro dev run dags unpause replicate_stackoverflow_to_target   # Enable DAG scheduling
astro dev run dags trigger replicate_stackoverflow_to_target   # Manual trigger

# SQL Server to PostgreSQL
astro dev run dags unpause replicate_stackoverflow_to_postgres
astro dev run dags trigger replicate_stackoverflow_to_postgres

astro dev run connections list                                  # View Airflow connections
```

### Database Inspection

```bash
# Check source database tables
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q \
  "USE StackOverflow2010; SELECT name FROM sys.tables ORDER BY name;"

# Check target database tables
docker exec stackoverflow-mssql-target /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q \
  "USE stackoverflow_target; SELECT name FROM sys.tables ORDER BY name;"
```

## Architecture Overview

### DAG Structure

**`replicate_stackoverflow_to_target.py`** - SQL Server to SQL Server replication
- Creates target database and schema
- Copies tables in dependency order
- Uses CSV-based bulk loading with batch INSERT
- Memory limit: 128MB in-memory buffer before disk spill
- Logs memory usage and disk spill events
- Aligns identity sequences after copy (DBCC CHECKIDENT)

**`replicate_stackoverflow_to_postgres.py`** - SQL Server to PostgreSQL replication
- Creates target database and schema with cross-database type mapping
- Converts SQL Server data types to PostgreSQL equivalents
- Uses PostgreSQL COPY command for efficient bulk loading
- Memory limit: 128MB in-memory buffer before disk spill
- Handles identity column conversion (IDENTITY → GENERATED ALWAYS AS IDENTITY)
- Resets PostgreSQL sequences after copy using setval()

### Table Dependency Order

```
Users → Badges
     → Posts → PostHistory
             → PostLinks
             → Comments
             → Votes
     → Tags (if present)
     → VoteTypes
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
- `stackoverflow_source` - Source SQL Server database (StackOverflow2010)
- `stackoverflow_target` - Target SQL Server database
- `stackoverflow_postgres_target` - Target PostgreSQL database

### Python Style
- Python 3.10+ with type hints
- PEP 8 spacing (4-space indents)
- `snake_case` for DAG IDs and task IDs
- Module-level uppercase constants for shared values

## Technology Stack

- **Apache Airflow 3** on Astro Runtime 3.1-3
- **Microsoft SQL Server 2022** (Developer Edition) for source database
- **PostgreSQL 16** for optional target database
- **Python 3.10+** with pymssql and pg8000 (pure Python drivers)
- **Docker** for database containerization
- **pytest** for DAG validation

## Troubleshooting

### Database File Permissions Issue (SQL Server)

If you encounter permission errors when attaching the StackOverflow2010 database:

**Problem:**
```
Msg 3415, Level 16, State 2, Server ..., Line 1
Database 'StackOverflow2010' cannot be upgraded because it is read-only,
has read-only files or the user does not have permissions to modify
some of the files. Make the database or files writeable, and rerun recovery.
```

**Root Cause:**
- Files copied with `docker cp` inherit host filesystem permissions
- SQL Server process runs as `mssql` user inside container
- Copied .mdf/.ldf files may not be owned by `mssql:mssql`
- Insufficient permissions prevent database upgrade/recovery

**Solution:**

Fix ownership and permissions as root user before attaching:

```bash
# Set ownership to mssql user
docker exec -u root stackoverflow-mssql-source chown mssql:mssql \
  /var/opt/mssql/data/StackOverflow2010.mdf \
  /var/opt/mssql/data/StackOverflow2010_log.ldf

# Set read/write permissions (660)
docker exec -u root stackoverflow-mssql-source chmod 660 \
  /var/opt/mssql/data/StackOverflow2010.mdf \
  /var/opt/mssql/data/StackOverflow2010_log.ldf
```

**Why This Works:**
- `chown mssql:mssql` grants ownership to SQL Server process user
- `chmod 660` allows read/write for owner and group, no access for others
- SQL Server can now upgrade database from 2008 format to 2022 format
- Database recovery completes successfully

**Note:** Running `chmod` as non-root will fail with "Operation not permitted"

### SQL Server SA Authentication Issue

If you encounter login failures when running sqlcmd commands:

**Problem:**
```
Sqlcmd: Error: Microsoft ODBC Driver 18 for SQL Server : Login failed for user 'sa'.
```

**Root Cause:**
- Password contains special characters (e.g., `StackOverflow123!`)
- Shell may interpret quotes inconsistently depending on context
- `docker exec` with quoted password fails in certain scenarios

**Solution:**

**For CREATE DATABASE and most queries:** Remove quotes from password parameter:
```bash
# ✓ CORRECT - No quotes
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P StackOverflow123! -C -Q "SELECT @@VERSION"

# ✗ WRONG - Quoted password fails
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "StackOverflow123!" -C -Q "SELECT @@VERSION"
```

**For Airflow connections:** Use quotes to prevent shell expansion:
```bash
astro dev run connections add stackoverflow_source \
  --conn-password "StackOverflow123!"  # Quotes needed here
```

**Why This Works:**
- Without quotes, shell passes password directly to sqlcmd
- sqlcmd receives the exact password string including `!`
- With quotes in `docker exec`, shell may escape or modify the password

**Alternative:** If authentication continues to fail:
1. Wait 15-30 seconds after container starts for SA password initialization
2. Restart container: `docker restart stackoverflow-mssql-source && sleep 30`
3. Use `-d` flag to specify database: `-d StackOverflow2010`

### PostgreSQL DAG with LocalExecutor

If you experience fork deadlocks with the PostgreSQL DAG when using LocalExecutor:

**Problem:** Tasks succeed but are marked as failed with `SIGKILL: -9`
**Cause:** psycopg2 (C extension) creates background threads that break during fork()
**Solution:** Use pg8000 (pure Python driver) instead

Already configured in `requirements.txt`. If you have issues:
1. Ensure `pg8000>=1.29.0` is in requirements.txt
2. Run `astro dev restart` to rebuild with pg8000
3. Connection will automatically use pg8000 when available

**Alternative solutions:**
- Use CeleryExecutor (for production)
- Use KubernetesExecutor (for cloud deployments)
- Use `astro dev run dags test` for development testing

### Empty String to NULL Conversion Issue (PostgreSQL)

If you encounter `NotNullViolation` errors during PostgreSQL replication:

**Problem:**
```
psycopg2.errors.NotNullViolation: null value in column "Body" of relation "Posts" violates not-null constraint
DETAIL: Failing row contains (...Body = null...)
```

**Root Cause:**
- SQL Server allows empty strings ('') in NOT NULL columns
- During CSV export/import, empty strings are converted to NULL values
- PostgreSQL enforces NOT NULL constraints strictly, rejecting NULL values
- Approximately 10 rows in Posts table have empty Body values in StackOverflow2010

**Solution (Implemented in `dags/replicate_stackoverflow_to_postgres.py`):**

Modified schema creation to allow NULL for text/varchar columns even when source schema defines NOT NULL:

```python
# Special case: Allow NULL for text columns even if source says NOT NULL
# to handle empty strings that get converted to NULL during CSV export
if is_nullable == 'NO' and pg_type not in ('TEXT', 'VARCHAR') and not pg_type.startswith('VARCHAR('):
    col_def += " NOT NULL"
else:
    col_def += " NULL"
```

**Why This Works:**
- Empty strings in source become NULL in target (acceptable data loss)
- Preserves data integrity for truly NULL values
- Prevents replication failures on edge cases
- Maintains NOT NULL constraints on non-text columns (integers, dates, etc.)

**Alternative Solutions:**
1. Pre-process source data to replace empty strings with a placeholder (e.g., '[EMPTY]')
2. Use COALESCE() in the SELECT query to replace empty strings with a default value
3. Modify target schema to use CHECK constraints instead of NOT NULL

## Stack Overflow Database Schema

### Main Tables

| Table | Description | Approximate Rows (2010) |
|-------|-------------|------------------------|
| Users | User accounts and profiles | 315,000 |
| Posts | Questions and answers | 1,700,000 |
| Comments | Comments on posts | 1,300,000 |
| Votes | Upvotes/downvotes | 4,300,000 |
| Badges | User achievements | 190,000 |
| PostHistory | Edit history | 2,800,000 |
| PostLinks | Related/duplicate posts | 100,000 |
| Tags | Question categorization | 13,000 |
| VoteTypes | Vote type lookup | ~15 |

### Key Relationships

- `Users.Id` → `Posts.OwnerUserId`, `Comments.UserId`, `Badges.UserId`
- `Posts.Id` → `Comments.PostId`, `Votes.PostId`, `PostHistory.PostId`, `PostLinks.PostId`
- `Posts.ParentId` → `Posts.Id` (answers reference questions)
- `VoteTypes.Id` → `Votes.VoteTypeId`

## Key Files

- `dags/replicate_stackoverflow_to_target.py` - SQL Server to SQL Server replication
- `dags/replicate_stackoverflow_to_postgres.py` - SQL Server to PostgreSQL replication
- `include/stackoverflow/StackOverflow2010.mdf` - Source database file (8.4GB)
- `include/stackoverflow/StackOverflow2010_log.ldf` - Transaction log (256MB)
- `include/stackoverflow/Readme_2010.txt` - Database documentation
- `tests/dags/test_dag_example.py` - DAG validation tests
- `.astro/config.yaml` - Resource allocation settings
- `Dockerfile` - Airflow parallelism configuration
- `CLAUDE.md` - AI assistant guidance and setup instructions

## Verification Commands

### Row Count Verification

```bash
# Comprehensive row count check
docker exec stackoverflow-mssql-target /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q \
  "USE stackoverflow_target;
   SELECT 'Users' AS TableName, COUNT(*) AS RowCount FROM dbo.Users
   UNION ALL SELECT 'Posts', COUNT(*) FROM dbo.Posts
   UNION ALL SELECT 'Comments', COUNT(*) FROM dbo.Comments
   UNION ALL SELECT 'Votes', COUNT(*) FROM dbo.Votes
   UNION ALL SELECT 'Badges', COUNT(*) FROM dbo.Badges
   UNION ALL SELECT 'PostHistory', COUNT(*) FROM dbo.PostHistory
   UNION ALL SELECT 'PostLinks', COUNT(*) FROM dbo.PostLinks
   UNION ALL SELECT 'Tags', COUNT(*) FROM dbo.Tags
   UNION ALL SELECT 'VoteTypes', COUNT(*) FROM dbo.VoteTypes
   ORDER BY TableName;"
```

### Performance Metrics

Expected replication performance (StackOverflow2010):
- Total dataset: ~10GB (compressed) / ~8.4GB (uncompressed .mdf)
- Estimated runtime: 10-30 minutes (depending on hardware)
- Memory usage: <128MB per task (spills to disk above threshold)
- Largest tables: Votes (~4.3M rows), PostHistory (~2.8M rows), Posts (~1.7M rows)

## License and Attribution

The StackOverflow2010 database is provided under **CC-BY-SA 3.0** license:
- Source: https://archive.org/details/stackexchange
- Compiled by: Brent Ozar Unlimited (https://www.brentozar.com)
- You are free to share and adapt this database, even commercially
- Attribution required to Stack Exchange Inc. and original authors
