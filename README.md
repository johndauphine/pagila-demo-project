# Stack Overflow End-to-End Replication Pipeline Using Astro + Apache Airflow

## 1. Introduction

This project provides complete, production-quality data replication pipelines using Brent Ozar's StackOverflow2010 database, Astronomer (Astro CLI), Apache Airflow 3, and memory-efficient streaming ETL. It demonstrates enterprise-grade data engineering practices including:

- **Memory-capped streaming replication** (128MB buffer)
- **SQL Server-to-SQL Server** replication using pymssql
- **SQL Server-to-PostgreSQL** cross-database replication with automatic type mapping
- **Large-scale dataset handling** (8.4GB database, ~12 million rows across 9 tables)
- **Identity sequence alignment** after data copy
- **Dependency-aware table ordering** to maintain referential integrity
- **Production-ready error handling** with configurable retries
- **Fork-safe database drivers** (pg8000 for PostgreSQL on LocalExecutor)

**Database Source:** Brent Ozar's StackOverflow2010 (2008-2010 data)
- Users: ~315K
- Posts: ~1.7M
- Comments: ~1.3M
- Votes: ~4.3M
- Badges: ~190K
- PostHistory: ~2.8M

---

## 2. Platform Requirements

### Supported Architectures

| Component | AMD64/x86_64 | ARM64 (Apple Silicon) |
|-----------|--------------|----------------------|
| **Airflow** | ✅ Native | ✅ Native |
| **PostgreSQL target** | ✅ Native | ✅ Native |
| **SQL Server source** | ✅ Native | ⚠️ Emulated (Rosetta 2) |
| **SQL Server target** | ✅ Native | ⚠️ Emulated (Rosetta 2) |

> **⚠️ macOS ARM64 (M1/M2/M3/M4) Users:**
>
> SQL Server 2022 does NOT support ARM64 natively. It runs in x86_64 emulation mode on Apple Silicon:
> - **Performance**: Slower (emulation overhead)
> - **Stability**: Can crash with large datasets (>100K rows)
> - **Recommended**:
>   - For SQL Server → PostgreSQL: Run SQL Server source on cloud AMD64 VM (see Section 9.7)
>   - For SQL Server → SQL Server: Use full AMD64 hardware or cloud VMs
> - **Alternative**: Use PostgreSQL → PostgreSQL pipeline (fully native ARM64, no emulation)

---

## 3. Prerequisites

- **Docker Desktop** - For running SQL Server containers
- **Astro CLI** - Astronomer's local development tool
- **Python 3.10+** - For Airflow DAGs
- **7zip** - For extracting the database archive (brew install p7zip on macOS)
- **8GB+ available disk space** - For the StackOverflow2010 database

### Required Python Packages

Add these to your Astro project's `requirements.txt`:
- `apache-airflow-providers-microsoft-mssql`
- `apache-airflow-providers-postgres`
- `apache-airflow-providers-common-sql`
- `pymssql` (SQL Server driver)
- `pg8000>=1.29.0` (PostgreSQL driver - pure Python, fork-safe for LocalExecutor)

Then rebuild/restart your Astro environment:

```bash
astro dev restart
```

---

## 4. Download and Extract Stack Overflow Database

### 4.1 Download StackOverflow2010

Download the 1GB compressed database file:

```bash
mkdir -p include/stackoverflow
cd include/stackoverflow
curl -L -o StackOverflow2010.7z https://downloads.brentozar.com/StackOverflow2010.7z
```

### 4.2 Extract Database Files

```bash
7z x StackOverflow2010.7z
```

This extracts:
- `StackOverflow2010.mdf` (8.4GB) - Main database file
- `StackOverflow2010_log.ldf` (256MB) - Transaction log
- `Readme_2010.txt` - Documentation

---

## 5. Start SQL Server Containers

### 5.1 Start Astro Environment

```bash
astro dev start
```

This creates the Airflow environment and a Docker network for inter-container communication.

### 5.2 Start Source SQL Server Container

```bash
docker run -d \
  --name stackoverflow-mssql-source \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -v "$(pwd)/include/stackoverflow":/var/opt/mssql/backup \
  -p 1433:1433 \
  mcr.microsoft.com/azure-sql-edge:latest
```

### 5.3 Start Target SQL Server Container

```bash
docker run -d \
  --name stackoverflow-mssql-target \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -p 1434:1433 \
  mcr.microsoft.com/azure-sql-edge:latest
```

> **Note**: Azure SQL Edge is used for ARM64 (Apple Silicon) and AMD64 compatibility. It's a lightweight version of SQL Server optimized for edge computing and development.
>
> **⚠️ Important**: Azure SQL Edge has known stability issues on ARM64 with large datasets. The `--memory="4g"` flag allocates 4GB RAM to improve stability. See section 11.5 for detailed troubleshooting if containers crash.

### 5.4 Connect Containers to Astro Network

```bash
# Discover the Astro network name
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'stackoverflow-demo-project.*_airflow')

# Connect both containers
docker network connect $ASTRO_NETWORK stackoverflow-mssql-source
docker network connect $ASTRO_NETWORK stackoverflow-mssql-target
```

---

## 6. Attach Source Database

Copy the database files into the source container and attach the database:

```bash
# Create data directory
docker exec stackoverflow-mssql-source mkdir -p /var/opt/mssql/data

# Copy database files
docker cp include/stackoverflow/StackOverflow2010.mdf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2010_log.ldf stackoverflow-mssql-source:/var/opt/mssql/data/

# Attach the database
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q \
  "CREATE DATABASE StackOverflow2010 ON (FILENAME = '/var/opt/mssql/data/StackOverflow2010.mdf'), (FILENAME = '/var/opt/mssql/data/StackOverflow2010_log.ldf') FOR ATTACH;"
```

### 6.1 Verify Source Database

```bash
# List tables in source database
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q \
  "USE StackOverflow2010; SELECT name FROM sys.tables ORDER BY name;"
```

Expected tables:
- Badges
- Comments
- PostHistory
- PostLinks
- Posts
- Tags
- Users
- VoteTypes
- Votes

---

## 7. Configure Airflow Connections

### 7.1 Add Source Connection

```bash
astro dev run connections add stackoverflow_source \
  --conn-type mssql \
  --conn-host stackoverflow-mssql-source \
  --conn-port 1433 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema StackOverflow2010
```

### 7.2 Add Target Connection

```bash
astro dev run connections add stackoverflow_target \
  --conn-type mssql \
  --conn-host stackoverflow-mssql-target \
  --conn-port 1433 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema master
```

### 7.3 Verify Connections

```bash
astro dev run connections list
```

You should see:
- `stackoverflow_source` - Source SQL Server (StackOverflow2010)
- `stackoverflow_target` - Target SQL Server

---

## 8. Run the Replication DAG

### 8.1 Unpause and Trigger DAG

```bash
# Enable the DAG
astro dev run dags unpause replicate_stackoverflow_to_target

# Trigger manual run
astro dev run dags trigger replicate_stackoverflow_to_target
```

### 8.2 Monitor DAG Execution

Access the Airflow UI at **http://localhost:8080**

- **Username**: `admin`
- **Password**: `admin`

Navigate to:
1. **DAGs** → `replicate_stackoverflow_to_target`
2. **Graph View** to see task dependencies
3. **Logs** to view detailed execution logs

### 8.3 Expected Runtime

- **Small tables** (Users, Badges, Tags): ~5-10 seconds each
- **Medium tables** (Posts, Comments): ~30-60 seconds each
- **Large tables** (Votes, PostHistory): ~2-5 minutes each
- **Total pipeline**: 10-30 minutes (hardware-dependent)

---

## 9. Verify Replication

### 9.1 Row Count Verification

```bash
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

### 9.2 Expected Row Counts

| Table | Expected Rows |
|-------|--------------|
| Badges | ~190,000 |
| Comments | ~1,300,000 |
| PostHistory | ~2,800,000 |
| PostLinks | ~100,000 |
| Posts | ~1,700,000 |
| Tags | ~13,000 |
| Users | ~315,000 |
| VoteTypes | ~15 |
| Votes | ~4,300,000 |

### 9.3 Sample Data Query

```bash
# Get top 10 users by reputation
docker exec stackoverflow-mssql-target /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q \
  "USE stackoverflow_target; SELECT TOP 10 DisplayName, Reputation FROM dbo.Users ORDER BY Reputation DESC;"
```

---

## 10. Alternative Setup: SQL Server to PostgreSQL Replication

This setup replicates data from SQL Server to PostgreSQL, providing cross-platform compatibility and cost benefits.

> **⚠️ IMPORTANT: macOS ARM64 (Apple Silicon) Limitation**
>
> **SQL Server 2022 does NOT support ARM64 architecture natively.** On macOS with M-series chips (M1, M2, M3, M4):
> - SQL Server source runs in **x86_64 emulation mode** (Rosetta 2 translation layer)
> - Uses `--platform linux/amd64` flag to force emulation
> - **Performance**: 🐌 Slower than native (emulation overhead)
> - **Stability**: ⚠️ Can be unstable, may crash during heavy operations
> - **Resource usage**: Higher CPU/memory consumption due to emulation
>
> **Recommended for macOS ARM64 users:**
> 1. **Cloud-based source** (BEST): Run SQL Server on AWS/Azure/GCP AMD64 VM, connect Airflow to it
> 2. **Accept limitations**: Use emulated SQL Server locally for small datasets only (<10K rows)
> 3. **Alternative**: Use PostgreSQL → PostgreSQL pipeline (fully native ARM64, no emulation)
>
> For **production workloads** or **large datasets** on ARM64, use Option 1 (cloud-based source).

### 10.1 Benefits

- **PostgreSQL target**: Runs natively on ARM64 (Apple Silicon), AMD64, and all major operating systems
- **No licensing costs**: PostgreSQL is open source
- **Better performance** for analytical workloads and data warehousing (when source is not emulated)
- **Production-ready**: Uses fork-safe pg8000 driver for LocalExecutor compatibility

### 10.2 Setup PostgreSQL Target

```bash
# Start PostgreSQL 16 target container
docker run -d \
  --name stackoverflow-postgres-target \
  -e "POSTGRES_PASSWORD=StackOverflow123!" \
  -e "POSTGRES_USER=postgres" \
  -e "POSTGRES_DB=stackoverflow_target" \
  -p 5432:5432 \
  postgres:16

# Connect to Astro network
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'stackoverflow.*_airflow')
docker network connect $ASTRO_NETWORK stackoverflow-postgres-target
```

### 10.3 Create PostgreSQL Connection

```bash
astro dev run connections add stackoverflow_postgres_target \
  --conn-type postgres \
  --conn-host stackoverflow-postgres-target \
  --conn-port 5432 \
  --conn-login postgres \
  --conn-password "StackOverflow123!" \
  --conn-schema stackoverflow_target
```

### 10.4 Run PostgreSQL Replication DAG

```bash
# Enable and trigger the PostgreSQL DAG
astro dev run dags unpause replicate_stackoverflow_to_postgres
astro dev run dags trigger replicate_stackoverflow_to_postgres
```

### 10.5 Verify PostgreSQL Replication

```bash
# Check row counts in PostgreSQL
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT 'Users' AS table_name, COUNT(*) AS row_count FROM \"Users\"
   UNION ALL SELECT 'Posts', COUNT(*) FROM \"Posts\"
   UNION ALL SELECT 'Comments', COUNT(*) FROM \"Comments\"
   UNION ALL SELECT 'Votes', COUNT(*) FROM \"Votes\"
   UNION ALL SELECT 'Badges', COUNT(*) FROM \"Badges\"
   ORDER BY table_name;"
```

### 10.6 PostgreSQL-Specific Features

**Data Type Mapping:**
| SQL Server | PostgreSQL |
|-----------|-----------|
| NVARCHAR(MAX) | TEXT |
| NVARCHAR(n) | VARCHAR(n) |
| DATETIME | TIMESTAMP |
| BIT | BOOLEAN |
| INT | INTEGER |
| BIGINT | BIGINT |
| IDENTITY(1,1) | GENERATED ALWAYS AS IDENTITY |

**Key Differences:**
- Uses PostgreSQL **COPY** command for faster bulk loading
- Automatic data type conversion
- Case-sensitive table/column names (quoted identifiers: `"Users"` not `users`)
- Uses **pg8000** driver (pure Python, fork-safe for Airflow LocalExecutor)
- No need for `SET IDENTITY_INSERT` equivalent
- Sequence management with **setval()** instead of **DBCC CHECKIDENT**

### 10.7 Cloud-Based SQL Server Source (Recommended for macOS ARM64)

For best performance and stability on macOS ARM64, run SQL Server source on a cloud AMD64 VM:

**Example: AWS EC2 Setup**
```bash
# 1. Launch Ubuntu AMD64 instance on AWS (t3.medium or larger)
# 2. Install Docker on the VM
# 3. Run SQL Server container on the VM
docker run -d --name stackoverflow-mssql-source \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -e "MSSQL_PID=Developer" \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

# 4. Update security group to allow port 1433 from your IP
# 5. Update Airflow connection to use cloud VM IP
astro dev run connections add stackoverflow_source \
  --conn-type mssql \
  --conn-host <ec2-public-ip-or-dns> \
  --conn-port 1433 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema StackOverflow2010
```

**Benefits:**
- ✅ Native AMD64 performance (no emulation)
- ✅ Stable for large datasets
- ✅ Production-like setup
- ✅ PostgreSQL target still runs locally on native ARM64

### 10.8 Troubleshooting PostgreSQL DAG

**Issue:** Tasks fail with `SIGKILL: -9` error

**Cause:** Using psycopg2 (C extension) with LocalExecutor causes fork deadlocks

**Solution:** Ensure pg8000 is installed:
```bash
# Check requirements.txt includes:
pg8000>=1.29.0

# Restart Airflow
astro dev restart
```

PostgresHook will automatically use pg8000 when available, avoiding fork issues.

**Issue:** SQL Server source crashes on macOS ARM64

**Cause:** SQL Server 2022 runs in x86_64 emulation mode on ARM64

**Solution:** Use cloud-based SQL Server source (see Section 10.7) or accept limitations for small datasets only.

---

## 11. Architecture Overview

### 11.1 DAG Structure

**File**: `dags/replicate_stackoverflow_to_target.py` (SQL Server → SQL Server)

**Key Features**:
- Memory-capped streaming (128MB buffer)
- CSV-based bulk loading with batch INSERT
- Automatic schema creation on target
- Identity sequence alignment (DBCC CHECKIDENT)
- Parallel task execution (up to 16 concurrent tasks)
- Dependency-aware table ordering

**File**: `dags/replicate_stackoverflow_to_postgres.py` (SQL Server → PostgreSQL)

**Key Features**:
- Cross-database data type mapping (SQL Server → PostgreSQL)
- PostgreSQL COPY command for efficient bulk loading
- Automatic data type conversion (NVARCHAR → VARCHAR, DATETIME → TIMESTAMP, etc.)
- Identity column conversion (IDENTITY → GENERATED ALWAYS AS IDENTITY)
- Sequence management with setval()
- Fork-safe pg8000 driver for LocalExecutor compatibility

### 11.2 Table Replication Order

Tables are replicated in dependency order to maintain referential integrity:

```
1. VoteTypes (lookup table, no dependencies)
2. Users (parent table)
3. Badges (depends on Users)
4. Tags (independent)
5. Posts (depends on Users)
6. PostHistory (depends on Posts)
7. PostLinks (depends on Posts)
8. Comments (depends on Posts, Users)
9. Votes (depends on Posts, VoteTypes)
```

### 11.3 Memory Management

- **SpooledTemporaryFile**: Uses 128MB in-memory buffer
- **Disk Spillover**: Automatically writes to disk when buffer exceeds threshold
- **Batch Processing**: Large tables processed in streaming fashion
- **Resource Limits**: Configurable in `.astro/config.yaml`

---

## 12. Project Structure

```
stackoverflow-demo-project/
├── dags/
│   ├── replicate_stackoverflow_to_target.py     # SQL Server → SQL Server DAG
│   └── replicate_stackoverflow_to_postgres.py   # SQL Server → PostgreSQL DAG
├── include/
│   └── stackoverflow/
│       ├── StackOverflow2010.mdf               # 8.4GB database file
│       ├── StackOverflow2010_log.ldf           # 256MB transaction log
│       └── Readme_2010.txt                     # Database documentation
├── tests/
│   └── dags/
│       └── test_dag_example.py                 # DAG validation tests
├── .astro/
│   └── config.yaml                             # Resource allocation
├── Dockerfile                                  # Airflow configuration
├── requirements.txt                            # Python dependencies
├── CLAUDE.md                                   # Claude Code instructions
├── AGENTS.md                                   # AI agent guidelines
└── README.md                                   # This file
```

---

## 13. Troubleshooting

### 13.1 Database Connection Issues

**Problem**: "Login failed for user 'sa'"

**Solution**:
```bash
# Verify SQL Server is running
docker ps | grep stackoverflow

# Check SQL Server logs
docker logs stackoverflow-mssql-source

# Wait 30 seconds for SQL Server to fully start
sleep 30
```

### 13.2 Database Attach Fails

**Problem**: "Unable to open the physical file" or "Operating system error 5"

**Solution**:
```bash
# Ensure files are in the correct location
docker exec stackoverflow-mssql-source ls -lh /var/opt/mssql/data/

# Check file permissions
docker exec stackoverflow-mssql-source chown mssql:mssql /var/opt/mssql/data/*.mdf
docker exec stackoverflow-mssql-source chown mssql:mssql /var/opt/mssql/data/*.ldf
```

### 13.3 Memory Issues

**Problem**: "Out of memory" or slow performance

**Solution**: Increase Docker memory allocation
- Docker Desktop → Settings → Resources → Memory: 8GB+
- Edit `.astro/config.yaml` to reduce parallelism

### 13.4 DAG Import Errors

**Problem**: DAG doesn't appear in Airflow UI

**Solution**:
```bash
# Check DAG validation
astro dev run dags list

# View import errors
astro dev run dags list-import-errors

# Restart Airflow
astro dev restart
```

### 13.5 Azure SQL Edge Stability Issues (IMPORTANT)

**Problem**: SQL Server container crashes with `SIGABRT` or exits unexpectedly during:
- Database initialization
- Heavy write operations (large tables)
- Random crashes during normal operations

**Symptoms**:
```bash
docker ps -a | grep stackoverflow-mssql-target
# Shows: Exited (1) X minutes ago

docker logs stackoverflow-mssql-target
# Shows: "This program has encountered a fatal error"
# Shows: "Signal: SIGABRT - Aborted (6)"
```

**Root Cause**: Azure SQL Edge has known stability issues on ARM64 architecture (Apple Silicon) when handling:
- Large datasets (multi-million row tables)
- Heavy concurrent writes
- Large transaction logs

**Solutions**:

**Option 1: Use Full SQL Server 2022 (Recommended for Production)**
```bash
# Replace Azure SQL Edge with full SQL Server 2022
# Note: Requires AMD64/x86_64 architecture (not Apple Silicon)
docker run -d --name stackoverflow-mssql-target \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -p 1434:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
```

**Option 2: Increase Memory Allocation (⚠️ NOT EFFECTIVE)**
```bash
# Allocate more memory to containers (4GB minimum)
docker run -d --name stackoverflow-mssql-target \
  --memory="4g" \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -p 1434:1433 \
  mcr.microsoft.com/azure-sql-edge:latest
```
**Update 2025-11-08**: Testing with 4GB and 8GB RAM confirmed that memory allocation does NOT resolve the ARM64 stability issues. Both configurations crashed during initialization. The problem is architectural incompatibility, not resource limitation.

**Option 3: Use Smaller Dataset**
- Use StackOverflow2010 database (10GB, ~12M rows) - current setup
- Or try filtering to fewer tables
- Consider using a subset of data for testing

**Option 4: Run on AMD64 Architecture**
- Use a cloud VM (AWS EC2, Azure VM, Google Compute Engine)
- Use Docker Desktop on Intel/AMD-based machine
- Azure SQL Edge is more stable on x86_64 architecture

**Workarounds for Development/Testing**:
1. **Restart containers frequently** if they crash
2. **Process tables in smaller batches** (already implemented with periodic commits)
3. **Monitor container health**:
   ```bash
   # Check if container is still running
   watch -n 5 'docker ps | grep stackoverflow'

   # Auto-restart on failure
   docker run -d --restart=unless-stopped ...
   ```

**Test Results**:
- ✅ Small tables (<1000 rows): Reliable on ARM64 with Azure SQL Edge
- ✅ Medium tables (10K-100K rows): Mostly reliable on ARM64
- ⚠️ Large tables (>100K rows): Frequent crashes on ARM64
- ❌ Very large tables (>1M rows): High crash rate on ARM64
- ❌ **8GB RAM allocation**: Containers crash during initialization (tested 2025-11-08)

**Memory Testing Results**:
| RAM Per Container | Initialization | Small Tables | Large Tables |
|-------------------|----------------|--------------|--------------|
| 4GB | ⚠️ Unstable | ✅ Works | ❌ Crashes |
| 8GB | ❌ **Crashes** | N/A | N/A |

**Note**: The DAG code itself is production-ready and works correctly. The issue is solely with Azure SQL Edge runtime stability on ARM64, not the replication logic. Memory allocation does NOT resolve the architectural incompatibility.

---

## 14. Development and Testing

### 14.1 Run DAG Tests

```bash
astro dev run pytest tests/dags -v
```

### 14.2 Dry-Run DAG

```bash
astro dev run dags test replicate_stackoverflow_to_target 2025-01-01
```

### 14.3 View Logs

```bash
# Scheduler logs
docker logs -f $(docker ps | grep scheduler | awk '{print $1}')

# Task logs (via Airflow UI)
http://localhost:8080 → DAGs → replicate_stackoverflow_to_target → Logs
```

---

## 15. Cleanup

### 15.1 Stop Containers

```bash
# Stop Airflow
astro dev stop

# Stop and remove SQL Server containers
docker stop stackoverflow-mssql-source stackoverflow-mssql-target
docker rm stackoverflow-mssql-source stackoverflow-mssql-target
```

### 15.2 Remove Database Files (Optional)

```bash
# Remove compressed archive (keep .mdf and .ldf)
rm include/stackoverflow/StackOverflow2010.7z

# Remove all database files (if starting fresh)
rm -rf include/stackoverflow/
```

---

## 16. License and Attribution

### 16.1 Stack Overflow Database

The StackOverflow2010 database is provided under **CC-BY-SA 3.0** license:
- **Source**: Stack Exchange Data Dump (https://archive.org/details/stackexchange)
- **Compiled by**: Brent Ozar Unlimited (https://www.brentozar.com)
- **License**: Creative Commons Attribution-ShareAlike 3.0 (http://creativecommons.org/licenses/by-sa/3.0/)

**You are free to**:
- Share — copy and redistribute the material
- Adapt — remix, transform, and build upon the material for any purpose, even commercially

**Under the following terms**:
- Attribution — You must give appropriate credit to Stack Exchange Inc. and original authors
- ShareAlike — If you remix or transform the material, you must distribute under the same license

### 16.2 Project Code

This replication pipeline project code is provided as-is for educational and production use.

---

## 17. Additional Resources

- **Brent Ozar's Blog**: https://www.brentozar.com/archive/category/tools/stack-overflow-database/
- **Stack Exchange Data Dump**: https://archive.org/details/stackexchange
- **Apache Airflow Docs**: https://airflow.apache.org/docs/
- **Astronomer Docs**: https://docs.astronomer.io/
- **pymssql Documentation**: https://pymssql.readthedocs.io/

---

## 18. Next Steps

1. **Add Incremental Loads**: Modify DAG to support CDC or timestamp-based updates
2. **Add Data Quality Checks**: Implement Great Expectations or custom validation
3. **Schedule Regular Syncs**: Configure DAG schedule_interval for automated runs
4. **Add Alerting**: Configure Airflow email/Slack alerts for failures
5. **Optimize Performance**: Tune batch sizes, parallelism, and memory buffers
6. **Add Monitoring**: Integrate with Prometheus, Grafana, or Datadog

---

**Questions or Issues?** Check the troubleshooting section or review the Airflow logs for detailed error messages.
