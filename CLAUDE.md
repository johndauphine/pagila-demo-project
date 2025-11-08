# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## ⚠️ IMPORTANT: Azure SQL Edge Stability Warning

**Azure SQL Edge has known stability issues on ARM64 (Apple Silicon) architecture:**
- Container may crash during database initialization or heavy writes
- Symptoms: Container exits with code 1, `SIGABRT` errors in logs
- Affects: Large table replication (>100K rows), especially Users, Posts, Votes tables
- **Tested 2025-11-08**: Even 8GB RAM allocation does NOT prevent initialization crashes

**Recommendations:**
1. **Production**: Use full SQL Server 2022 on AMD64/x86_64 architecture (REQUIRED)
2. **Development on ARM64**: ⚠️ NOT RECOMMENDED - crashes even with 8GB RAM
3. **Alternative**: Test on cloud VM (AWS, Azure, GCP) with AMD64 architecture

**Memory Testing Results:**
- 4GB RAM: Unstable, crashes during heavy operations
- 8GB RAM: Crashes during initialization (before any data operations)
- **Conclusion**: Issue is architectural incompatibility, NOT memory limitation

**If you must test on ARM64:**
```bash
docker run -d --name stackoverflow-mssql-target \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -p 1434:1433 mcr.microsoft.com/azure-sql-edge:latest
```
Expect frequent crashes. Restart containers as needed.

See README.md section 11.5 for detailed troubleshooting.

---

## Quick Start: Complete Environment Setup

### Step 1: Start Astro
```bash
astro dev start
```

### Step 2: Set Up SQL Server Databases

**SQL Server-to-SQL Server Replication Pipeline:**

```bash
# Start SQL Server source with StackOverflow2010 database
# Note: The .mdf and .ldf files are in include/stackoverflow/
# Allocate 4GB RAM for stability (especially important on ARM64)
docker run -d --name stackoverflow-mssql-source \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -v "$(pwd)/include/stackoverflow":/var/opt/mssql/backup \
  -p 1433:1433 mcr.microsoft.com/azure-sql-edge:latest

# Start SQL Server target (4GB RAM for heavy write operations)
docker run -d --name stackoverflow-mssql-target \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -p 1434:1433 mcr.microsoft.com/azure-sql-edge:latest

# Connect to Astro network
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'stackoverflow-demo-project.*_airflow')
docker network connect $ASTRO_NETWORK stackoverflow-mssql-source
docker network connect $ASTRO_NETWORK stackoverflow-mssql-target
```

### Step 3: Attach Source Database

```bash
# Copy database files into source container and attach
docker exec -it stackoverflow-mssql-source mkdir -p /var/opt/mssql/data
docker cp include/stackoverflow/StackOverflow2010.mdf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2010_log.ldf stackoverflow-mssql-source:/var/opt/mssql/data/

# Attach the database
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q \
  "CREATE DATABASE StackOverflow2010 ON (FILENAME = '/var/opt/mssql/data/StackOverflow2010.mdf'), (FILENAME = '/var/opt/mssql/data/StackOverflow2010_log.ldf') FOR ATTACH;"
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

## Project Overview

This is a **Stack Overflow End-to-End Data Replication Pipeline** using Apache Airflow 3 and Microsoft SQL Server. It demonstrates production-quality data engineering practices including memory-capped streaming replication, audit logging, and resource management for large-scale datasets.

**Key Components:**
- Source SQL Server database (port 1433) with StackOverflow2010 sample data (Brent Ozar edition)
- Target SQL Server database (port 1434)
- Apache Airflow DAGs for orchestration
- Audit trail and data validation
- Memory-efficient streaming replication with 128MB buffer

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
astro dev run pytest tests/dags                                  # Run DAG validation tests
astro dev run dags test replicate_stackoverflow_to_target <date>  # Dry-run specific DAG
```

### DAG Operations
```bash
astro dev run dags unpause replicate_stackoverflow_to_target   # Enable DAG scheduling
astro dev run dags trigger replicate_stackoverflow_to_target   # Manual trigger
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

**`replicate_stackoverflow_to_target.py`** - Memory-capped streaming replication
- Creates target database and schema
- Copies tables in dependency order
- Uses CSV-based bulk loading with batch INSERT
- Memory limit: 128MB in-memory buffer before disk spill
- Logs memory usage and disk spill events
- Aligns identity sequences after copy (DBCC CHECKIDENT)

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

### Python Style
- Python 3.10+ with type hints
- PEP 8 spacing (4-space indents)
- `snake_case` for DAG IDs and task IDs
- Module-level uppercase constants for shared values

## Technology Stack

- **Apache Airflow 3** on Astro Runtime 3.1-3
- **Microsoft SQL Server** (Azure SQL Edge) for source and target databases
- **Python 3.10+** with pymssql
- **Docker** for database containerization
- **pytest** for DAG validation

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

- `dags/replicate_stackoverflow_to_target.py` - Streaming replication logic
- `include/stackoverflow/StackOverflow2010.mdf` - Source database file (8.4GB)
- `include/stackoverflow/StackOverflow2010_log.ldf` - Transaction log (256MB)
- `include/stackoverflow/Readme_2010.txt` - Database documentation
- `tests/dags/test_dag_example.py` - DAG validation tests
- `.astro/config.yaml` - Resource allocation settings
- `Dockerfile` - Airflow parallelism configuration
- `AGENTS.md` - Repository guidelines for AI agents

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
