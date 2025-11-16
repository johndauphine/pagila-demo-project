# Stack Overflow End-to-End Replication Pipeline Using Astro + Apache Airflow

## 1. Introduction

This project provides complete, production-quality data replication pipelines using Brent Ozar's StackOverflow database, Astronomer (Astro CLI), Apache Airflow 3, and memory-efficient streaming ETL. It demonstrates enterprise-grade data engineering practices including:

### Key Features

- **Two replication strategies**: In-memory streaming and bulk file-based parallel loading
- **SQL Server-to-PostgreSQL** cross-database replication with automatic type mapping
- **Large-scale dataset handling** (StackOverflow2010: 8.4GB, ~12M rows; tested with StackOverflow2013: 33GB, 70M+ rows)
- **Maximum parallel execution** - all tables load simultaneously (no foreign key dependencies during load phase)
- **Memory-efficient streaming** (256MB buffer with disk spillover)
- **Bulk file partitioning** for very large tables (500K rows per partition)
- **PostgreSQL COPY command** for high-performance bulk loading
- **Identity sequence alignment** after data copy
- **Cross-database type mapping** (SQL Server → PostgreSQL automatic conversion)
- **Production-ready error handling** with configurable retries
- **Fork-safe database drivers** (pg8000 for PostgreSQL on LocalExecutor)

### Available DAGs

This project includes **two production-ready DAGs** for different use cases:

#### 1. **Streaming DAG** - `replicate_stackoverflow_to_postgres_parallel`
- **Best for**: Medium datasets, real-time-like replication
- **Strategy**: In-memory streaming with 256MB buffer
- **Approach**: Reads rows directly from source, buffers in memory, writes to target
- **Memory**: Efficient - uses SpooledTemporaryFile (spills to disk if needed)
- **Speed**: Fast - direct row streaming without intermediate files
- **Parallelism**: Full parallel - all 9 tables load simultaneously
- **Tables**: All 9 tables processed in parallel (no foreign key constraints during load)

#### 2. **Bulk Parallel DAG** - `replicate_stackoverflow_to_postgres_bulk_parallel`
- **Best for**: Very large datasets (millions of rows), maximum parallelism
- **Strategy**: Pre-partitioned CSV files with PostgreSQL COPY command
- **Approach**: Extracts to CSV files (500K rows per partition), loads in parallel batches
- **Memory**: Minimal - offloads data to shared volume (`/bulk_files/`)
- **Speed**: Very fast - PostgreSQL COPY is optimized for bulk loading
- **Parallelism**: Maximum - loads multiple partitions simultaneously per table
- **Tables**: Large tables partitioned (Votes, Posts, Comments), small tables loaded whole
- **Requirements**: Shared volume between Airflow and PostgreSQL containers

**Quick Comparison:**

| Feature | Streaming DAG | Bulk Parallel DAG |
|---------|---------------|-------------------|
| Dataset Size | All sizes | All sizes |
| Memory Usage | 256MB buffer per task | Minimal (disk-based) |
| Speed (2010) | 3m 36s | ~3-4 min |
| Speed (2013) | **18m 14s** | 30m 26s |
| Setup Complexity | **Simple** | Moderate (requires shared volume) |
| Parallelism | Full parallel (all tables) | Full parallel (all tables) |
| Best Use Case | **✅ ALL use cases (recommended)** | ❌ Not recommended (slower) |

**✅ Recommendation:** Use **Streaming DAG** - proven faster on both small (12M) and large (106M) datasets

**Database Source:** Brent Ozar's StackOverflow databases
- **StackOverflow2010** (8.4GB, ~12M rows) - Primary test dataset
  - Users: ~315K
  - Posts: ~1.7M
  - Comments: ~1.3M
  - Votes: ~4.3M
  - Badges: ~190K
  - PostHistory: ~2.8M
- **StackOverflow2013** (33GB, 70M+ rows) - Large-scale validation dataset

---

## 2. Platform Requirements

### Supported Architectures

| Component | AMD64/x86_64 | ARM64 (Apple Silicon) |
|-----------|--------------|----------------------|
| **Airflow** | ✅ Native | ✅ Native |
| **PostgreSQL target** | ✅ Native | ✅ Native |
| **SQL Server source** | ✅ Native | ⚠️ Emulated (Rosetta 2) |

> **⚠️ macOS ARM64 (M1/M2/M3/M4) Users:**
>
> SQL Server 2022 does NOT support ARM64 natively. It runs in x86_64 emulation mode on Apple Silicon:
> - **Performance**: Slower (emulation overhead)
> - **Stability**: Generally stable for small-medium datasets
> - **Recommended Setup**:
>   - **Primary**: SQL Server → PostgreSQL (PostgreSQL runs natively on ARM64)
>   - **Cloud**: Run SQL Server source on AWS/Azure/GCP AMD64 VM for large datasets
>   - **Alternative**: PostgreSQL → PostgreSQL (fully native, no emulation)
> - **Tested**: Successfully replicated StackOverflow2010 (8.4GB, ~12M rows) on ARM64

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
  --platform linux/amd64 \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=StackOverflow123!" \
  -e "MSSQL_PID=Developer" \
  -v "$(pwd)/include/stackoverflow":/var/opt/mssql/backup \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest
```

> **Note**: Using SQL Server 2022 Developer Edition (full SQL Server, not Azure SQL Edge) for better stability and performance. The `--platform linux/amd64` flag enables emulation on ARM64 systems.
>
> **Memory**: 4GB RAM allocation recommended for handling large datasets.

### 5.3 Start PostgreSQL Target Container

```bash
docker run -d \
  --name stackoverflow-postgres-target \
  -e "POSTGRES_PASSWORD=StackOverflow123!" \
  -e "POSTGRES_USER=postgres" \
  -e "POSTGRES_DB=stackoverflow_target" \
  -p 5433:5432 \
  postgres:16
```

> **Note**: PostgreSQL 16 is used as the target database. It runs natively on all platforms (ARM64, AMD64) with excellent performance and stability. Port 5433 is used on the host to avoid conflicts with existing PostgreSQL installations.

### 5.4 (Optional) Setup Shared Volume for Bulk DAG

If you plan to use the **bulk parallel DAG**, create a shared volume:

```bash
# Create shared directory for bulk files
mkdir -p include/bulk_files

# Mount the volume when starting PostgreSQL
docker stop stackoverflow-postgres-target
docker rm stackoverflow-postgres-target

docker run -d \
  --name stackoverflow-postgres-target \
  -e "POSTGRES_PASSWORD=StackOverflow123!" \
  -e "POSTGRES_USER=postgres" \
  -e "POSTGRES_DB=stackoverflow_target" \
  -v "$(pwd)/include/bulk_files":/bulk_files \
  -p 5433:5432 \
  postgres:16
```

> **Note**: The bulk DAG uses `/bulk_files/` as a shared volume for partitioned CSV files. This allows PostgreSQL to directly read files generated by Airflow.

### 5.5 Connect Containers to Astro Network

```bash
# Discover the Astro network name
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'stackoverflow.*_airflow')

# Connect source SQL Server
docker network connect $ASTRO_NETWORK stackoverflow-mssql-source

# Connect PostgreSQL target
docker network connect $ASTRO_NETWORK stackoverflow-postgres-target
```

> **Network Architecture**: This creates a production-like setup where Airflow communicates with databases via Docker DNS (e.g., `stackoverflow-mssql-source:1433`), similar to how cloud environments work (AWS RDS, Azure SQL, etc.).

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

### 7.2 Add PostgreSQL Target Connection

```bash
astro dev run connections add stackoverflow_postgres_target \
  --conn-type postgres \
  --conn-host stackoverflow-postgres-target \
  --conn-port 5432 \
  --conn-login postgres \
  --conn-password "StackOverflow123!" \
  --conn-schema stackoverflow_target
```

### 7.3 Verify Connections

```bash
astro dev run connections list
```

You should see:
- `stackoverflow_source` - Source SQL Server (StackOverflow2010)
- `stackoverflow_postgres_target` - Target PostgreSQL database

---

## 8. Run the Replication DAG

### 8.1 Choose Your DAG

**Option 1: Streaming DAG** (Recommended - simpler setup, equivalent performance)
```bash
# Enable and trigger streaming DAG
astro dev run dags unpause replicate_stackoverflow_to_postgres_parallel
astro dev run dags trigger replicate_stackoverflow_to_postgres_parallel
```

**Option 2: Bulk Parallel DAG** (Alternative approach using shared volume)
```bash
# Enable and trigger bulk parallel DAG
astro dev run dags unpause replicate_stackoverflow_to_postgres_bulk_parallel
astro dev run dags trigger replicate_stackoverflow_to_postgres_bulk_parallel
```

> **Note**: Both DAGs perform similarly (~3-4 minutes for StackOverflow2010). Streaming DAG is recommended as it's simpler (no shared volume required). Bulk DAG may provide slight advantage on very large datasets (StackOverflow2013: 33GB+).

### 8.2 Monitor DAG Execution

Access the Airflow UI at **http://localhost:8080**

- **Username**: `admin`
- **Password**: `admin`

Navigate to:
1. **DAGs** → Select your DAG (`replicate_stackoverflow_to_postgres_parallel` or `replicate_stackoverflow_to_postgres_bulk_parallel`)
2. **Graph View** to see task dependencies and parallelism
3. **Logs** to view detailed execution logs

### 8.3 Expected Runtime

**Streaming DAG** (`replicate_stackoverflow_to_postgres_parallel`):
- **Small tables** (VoteTypes, PostTypes, LinkTypes): ~1-2 seconds each
- **Medium tables** (Users, Badges): ~5-15 seconds each
- **Large tables** (Posts, Comments, Votes): ~30-120 seconds each
- **Total pipeline**: **~3-4 minutes** for StackOverflow2010 (tested: 3m 36s)

**Bulk Parallel DAG** (`replicate_stackoverflow_to_postgres_bulk_parallel`):
- **Extraction + Loading**: **~3-4 minutes** for StackOverflow2010
- Uses shared volume and PostgreSQL COPY for maximum throughput

> **Note**: Both DAGs perform similarly due to full parallelism. Streaming DAG is simpler to set up (no shared volume required). Bulk DAG may have slight advantage on very large datasets (StackOverflow2013: 33GB, 70M+ rows) due to PostgreSQL COPY optimizations.

---

## 9. Verify Replication

### 9.1 Row Count Verification

```bash
# Check row counts in PostgreSQL
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT 'Users' AS table_name, COUNT(*) AS row_count FROM \"Users\"
   UNION ALL SELECT 'Posts', COUNT(*) FROM \"Posts\"
   UNION ALL SELECT 'Comments', COUNT(*) FROM \"Comments\"
   UNION ALL SELECT 'Votes', COUNT(*) FROM \"Votes\"
   UNION ALL SELECT 'Badges', COUNT(*) FROM \"Badges\"
   UNION ALL SELECT 'PostLinks', COUNT(*) FROM \"PostLinks\"
   UNION ALL SELECT 'VoteTypes', COUNT(*) FROM \"VoteTypes\"
   UNION ALL SELECT 'PostTypes', COUNT(*) FROM \"PostTypes\"
   UNION ALL SELECT 'LinkTypes', COUNT(*) FROM \"LinkTypes\"
   ORDER BY table_name;"
```

### 9.2 Expected Row Counts (StackOverflow2010)

| Table | Expected Rows |
|-------|--------------|
| Badges | ~190,000 |
| Comments | ~1,300,000 |
| PostLinks | ~100,000 |
| Posts | ~1,700,000 |
| Users | ~315,000 |
| Votes | ~4,300,000 |
| VoteTypes | ~15 |
| PostTypes | ~10 |
| LinkTypes | ~20 |

### 9.3 Sample Data Query

```bash
# Get top 10 users by reputation
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  'SELECT "DisplayName", "Reputation" FROM "Users" ORDER BY "Reputation" DESC LIMIT 10;'
```

### 9.4 Verify Data Types (PostgreSQL)

```bash
# Check table schema
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT column_name, data_type, character_maximum_length 
   FROM information_schema.columns 
   WHERE table_name = 'Users' 
   ORDER BY ordinal_position;"
```

You should see SQL Server types correctly mapped to PostgreSQL:
- `NVARCHAR(MAX)` → `text`
- `NVARCHAR(n)` → `character varying(n)`
- `DATETIME` → `timestamp without time zone`
- `INT` → `integer`
- `BIT` → `boolean`

---

## 10. DAG Architecture Details

### 10.1 Streaming DAG Architecture

**File**: `dags/replicate_stackoverflow_to_postgres_parallel.py`

**Key Features**:
- In-memory streaming with SpooledTemporaryFile (256MB buffer)
- Direct row-by-row streaming from source to target
- **Full parallel execution**: All 9 tables load simultaneously
  - No foreign key constraints during load phase
  - Maximum parallelism - all tables process concurrently
- Cross-database type mapping (SQL Server → PostgreSQL)
- PostgreSQL COPY command for efficient bulk loading
- Automatic sequence management with setval()
- Fork-safe pg8000 driver
- Foreign keys and indexes added after all data is loaded

**Data Flow**:
```
Source DB → Stream rows → Buffer (256MB) → Spill to disk if needed → PostgreSQL COPY → Target DB
```

**Task Dependencies**:
```
Reset Schema → Create Schema → [All 9 Tables in Parallel] → Convert to Logged → Align Sequences
```

### 10.2 Bulk Parallel DAG Architecture

**File**: `dags/replicate_stackoverflow_to_postgres_bulk_parallel.py`

**Key Features**:
- Pre-partitioned CSV file strategy (500K rows per partition)
- Shared volume between Airflow and PostgreSQL containers
- **Full parallel execution**: All tables load simultaneously
- Large tables automatically partitioned (Votes, Posts, Comments)
- Small tables loaded whole (VoteTypes, PostTypes, LinkTypes, etc.)
- No foreign key constraints during load phase
- PostgreSQL COPY command for ultra-fast bulk loading
- Foreign keys and indexes added after all data is loaded

**Data Flow**:
```
Source DB → Extract to CSV partitions → Shared volume (/bulk_files/) → PostgreSQL COPY (parallel) → Target DB
```

**Task Dependencies**:
```
Reset → Create Schema → Optimize PostgreSQL → [All 9 Tables in Parallel] → Add Indexes → Convert to Logged → Align Sequences
```

**Partitioning Strategy** (StackOverflow2010):
- **Votes**: ~9 partitions (4.3M rows, 500K per partition)
- **Posts**: ~4 partitions (1.7M rows)
- **Comments**: ~3 partitions (1.3M rows)
- **Users, Badges, PostLinks**: No partitioning (< 500K rows each)
- **VoteTypes, PostTypes, LinkTypes**: No partitioning (< 100 rows each)

> **Note**: For StackOverflow2013, partition counts scale proportionally: Votes (~80 partitions), Posts (~20 partitions), Comments (~14 partitions)

### 10.3 Cross-Database Type Mapping

Both DAGs automatically convert SQL Server types to PostgreSQL equivalents:

| SQL Server Type | PostgreSQL Type | Notes |
|----------------|-----------------|-------|
| NVARCHAR(MAX) | TEXT | Unlimited text |
| NVARCHAR(n) | VARCHAR(n) | Fixed-length text |
| DATETIME | TIMESTAMP | No timezone |
| INT | INTEGER | 32-bit integer |
| BIGINT | BIGINT | 64-bit integer |
| BIT | BOOLEAN | True/False |
| IDENTITY(1,1) | GENERATED ALWAYS AS IDENTITY | Auto-increment |

### 10.4 Table Replication Strategy

**Full Parallel Execution**:
All 9 tables load simultaneously without dependencies:

```
[VoteTypes, PostTypes, LinkTypes, Users, Badges, Posts, Comments, Votes, PostLinks] → All in parallel
```

**Key Design Decisions**:
- No foreign key constraints during load phase (added after all data is loaded)
- Maximum parallelism - all tables process concurrently
- Referential integrity enforced after load via indexes and constraints
- Significantly faster than sequential or grouped approaches

### 10.5 Memory Management

**Streaming DAG**:
- Uses SpooledTemporaryFile with 256MB in-memory buffer
- Automatically spills to disk when buffer exceeds threshold
- Memory-efficient for tables of any size

**Bulk Parallel DAG**:
- Minimal memory usage (CSV files written to disk)
- PostgreSQL COPY command reads directly from shared volume
- Airflow workers only manage task orchestration

---

## 11. Project Structure

```
stackoverflow-replication-pipeline/
├── dags/
│   ├── replicate_stackoverflow_to_postgres_parallel.py      # Streaming DAG (recommended)
│   └── replicate_stackoverflow_to_postgres_bulk_parallel.py # Bulk parallel DAG
├── include/
│   ├── bulk_files/                    # Shared volume for bulk DAG (partitioned CSVs)
│   │   ├── Badges_part000.csv
│   │   ├── Comments_part000.csv
│   │   ├── Posts_part000.csv
│   │   ├── Votes_part000.csv
│   │   └── ... (hundreds of partition files)
│   └── stackoverflow/
│       ├── StackOverflow2010.mdf      # 8.4GB database file
│       ├── StackOverflow2010_log.ldf  # 256MB transaction log
│       └── Readme.txt                 # Database documentation
├── tests/
│   └── dags/
│       └── test_dag_example.py        # DAG validation tests
├── docs/
│   ├── optimization-summary.md        # Performance optimization analysis
│   ├── parallel-dag-performance-analysis.md
│   ├── streaming-performance-analysis.md
│   └── troubleshooting-infrastructure.md
├── .astro/
│   ├── config.yaml                    # Resource allocation
│   └── test_dag_integrity_default.py  # Astro DAG integrity tests
├── Dockerfile                         # Airflow environment configuration
├── requirements.txt                   # Python dependencies
├── CLAUDE.md                          # AI assistant instructions
├── TESTING_RESULTS.md                 # Comprehensive test results
└── README.md                          # This file
```

---

## 12. Troubleshooting

## 12. Troubleshooting

### 12.1 Database Connection Issues

**Problem**: "Login failed for user 'sa'" or connection timeout

**Solution**:
```bash
# Verify SQL Server is running
docker ps | grep stackoverflow

# Check SQL Server logs
docker logs stackoverflow-mssql-source

# Wait for SQL Server to fully start (can take 30-60 seconds)
sleep 30

# Test connection
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "StackOverflow123!" -C -Q "SELECT @@VERSION"
```

### 12.2 Database Attach Fails

**Problem**: "Unable to open the physical file" or "Operating system error 5"

**Solution**:
```bash
# Ensure files are in the correct location
docker exec stackoverflow-mssql-source ls -lh /var/opt/mssql/data/

# Check file permissions
docker exec stackoverflow-mssql-source chown mssql:mssql /var/opt/mssql/data/*.mdf
docker exec stackoverflow-mssql-source chown mssql:mssql /var/opt/mssql/data/*.ldf
```

### 12.3 Memory Issues

**Problem**: "Out of memory" or slow performance

**Solution**: Increase Docker memory allocation
- Docker Desktop → Settings → Resources → Memory: 8GB+
- Edit `.astro/config.yaml` to adjust parallelism:
  ```yaml
  executor:
    type: local
    workers: 4  # Reduce from default 16 if memory constrained
  ```

### 12.4 DAG Import Errors

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

### 12.5 PostgreSQL COPY Permission Denied

**Problem**: "ERROR: could not open file for reading: Permission denied"

**Solution**: Ensure shared volume is properly mounted
```bash
# Check volume mount
docker inspect stackoverflow-postgres-target | grep -A 10 Mounts

# Verify files are accessible
docker exec stackoverflow-postgres-target ls -lh /bulk_files/

# Fix permissions if needed
chmod -R 755 include/bulk_files/
```

### 12.6 Fork Deadlock with psycopg2

**Problem**: Tasks fail with `SIGKILL: -9` or hang indefinitely

**Cause**: psycopg2 (C extension) has known fork-safety issues with Airflow LocalExecutor

**Solution**: Use pg8000 driver (already configured in requirements.txt)
```bash
# Verify pg8000 is installed
astro dev run pip list | grep pg8000

# If not installed, add to requirements.txt:
# pg8000>=1.29.0

# Restart Airflow
astro dev restart
```

### 12.7 SQL Server Emulation Issues on ARM64

**Problem**: SQL Server crashes or performs poorly on macOS ARM64

**Cause**: SQL Server 2022 runs in x86_64 emulation mode on Apple Silicon

**Solutions**:
1. **Use smaller datasets**: Test with subset of data first
2. **Increase Docker resources**: Allocate 8GB+ RAM to Docker Desktop
3. **Cloud-based source** (recommended): Run SQL Server on AWS/Azure AMD64 VM
4. **Alternative architecture**: Use PostgreSQL as source (fully native ARM64)

**Test SQL Server stability**:
```bash
# Monitor container status
watch -n 2 'docker ps -a | grep stackoverflow-mssql-source'

# Check for crashes in logs
docker logs stackoverflow-mssql-source | grep -i "error\|crash\|abort"
```

### 12.8 Bulk DAG Partition Files Not Found

**Problem**: Bulk DAG fails with "file not found" errors

**Solution**: 
1. Ensure shared volume is mounted correctly (see Section 5.4)
2. Verify partition files exist:
   ```bash
   ls -lh include/bulk_files/ | head -20
   ```
3. Check Airflow can access the files:
   ```bash
   docker exec -it $(docker ps | grep scheduler | awk '{print $1}') ls -lh /usr/local/airflow/include/bulk_files/
   ```

---

## 13. Development and Testing

### 13.1 Run DAG Tests

```bash
astro dev run pytest tests/dags -v
```

### 13.2 Dry-Run DAG

```bash
# Test streaming DAG
astro dev run dags test replicate_stackoverflow_to_postgres_parallel 2025-01-01

# Test bulk parallel DAG
astro dev run dags test replicate_stackoverflow_to_postgres_bulk_parallel 2025-01-01
```

### 13.3 View Logs

```bash
# Scheduler logs
docker logs -f $(docker ps | grep scheduler | awk '{print $1}')

# Webserver logs
docker logs -f $(docker ps | grep webserver | awk '{print $1}')

# Task logs (via Airflow UI)
# Navigate to: http://localhost:8080 → DAGs → Your DAG → Logs
```

### 13.4 Performance Monitoring

```bash
# Monitor container resource usage
docker stats stackoverflow-mssql-source stackoverflow-postgres-target

# Monitor disk space for bulk files
du -sh include/bulk_files/

# Check PostgreSQL activity
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT pid, usename, query, state FROM pg_stat_activity WHERE datname = 'stackoverflow_target';"
```

---

## 14. Cleanup

## 14. Cleanup

### 14.1 Stop Containers

```bash
# Stop Airflow
astro dev stop

# Stop database containers
docker stop stackoverflow-mssql-source stackoverflow-postgres-target
docker rm stackoverflow-mssql-source stackoverflow-postgres-target
```

### 14.2 Remove Database Files (Optional)

```bash
# Remove compressed archive (keep .mdf and .ldf)
rm include/stackoverflow/StackOverflow2010.7z

# Remove bulk partition files
rm -rf include/bulk_files/*

# Remove all database files (if starting fresh)
rm -rf include/stackoverflow/
```

### 14.3 Clean Docker Resources

```bash
# Remove unused volumes
docker volume prune

# Remove unused networks
docker network prune

# Remove all stopped containers
docker container prune
```

---

## 15. Performance Benchmarks

### 15.1 StackOverflow2010 (8.4GB, ~12M rows)

**Streaming DAG** (`replicate_stackoverflow_to_postgres_parallel`):
- **Total runtime**: **3m 36s (216 seconds)** - tested and verified
- **Memory usage**: Peak 256MB per task
- **Parallelism**: Full parallel (all 9 tables simultaneously)
- **Best for**: General use, simple setup (no shared volume required)

**Bulk Parallel DAG** (`replicate_stackoverflow_to_postgres_bulk_parallel`):
- **Total runtime**: **~3-4 minutes** (similar to streaming)
- **Bottleneck**: Slowest table determines runtime (Votes: ~3.2 minutes for 10M rows)
- **Disk usage**: ~5GB temporary CSV files
- **Best for**: Very large datasets where PostgreSQL COPY provides additional optimization

**Performance Notes**:
- Both DAGs achieve similar performance due to full parallelism (all 9 tables load simultaneously)
- Runtime determined by slowest table, not sum of all tables
- Streaming DAG is recommended for most use cases (simpler setup)

### 15.2 StackOverflow2013 (52GB, 106.5M rows) - **10x larger dataset**

**Tested: 2025-11-15**

**Streaming DAG** (`replicate_stackoverflow_to_postgres_parallel`):
- **Total runtime**: **18m 14s (1094 seconds)** ✅ **Tested and verified**
- **Throughput**: ~97,000 rows/second average
- **Bottleneck**: Posts table (17.1M rows) took 14m 9s - determines total runtime
- **Data verified**: All 106,542,570 rows transferred successfully

**Per-Table Performance**:
| Table | Rows | Time | Throughput |
|-------|------|------|------------|
| Posts | 17.1M | 14m 9s (849s) | 20K rows/sec (slowest) |
| Votes | 52.9M | 9m 13s (553s) | 96K rows/sec |
| Comments | 24.5M | 7m 14s (434s) | 56K rows/sec |
| Badges | 8.0M | 1m 29s (89s) | 90K rows/sec |
| Users | 2.5M | 56s | 44K rows/sec |
| PostLinks | 1.4M | 14s | 101K rows/sec |
| Small tables | 25 | <1s each | - |

**Bulk Parallel DAG** (`replicate_stackoverflow_to_postgres_bulk_parallel`):
- **Total runtime**: **30m 26s (1826 seconds)** ✅ **Tested and verified**
- **Bottleneck**: Posts table (17.1M rows) took 25m 33s
- **CSV overhead**: Writes ~50GB of temporary CSV files to shared volume
- **Data verified**: All 106,534,570 rows transferred successfully

**Per-Table Performance (Bulk)**:
| Table | Rows | Time | Notes |
|-------|------|------|-------|
| Posts | 17.1M | 25m 33s (1533s) | Slowest (CSV I/O overhead) |
| Votes | 52.9M | 15m 50s (950s) | Partitioned export |
| Comments | 24.5M | 11m 50s (710s) | Partitioned export |
| Badges | 8.0M | 1m 55s (115s) | |
| Users | 2.5M | 1m 21s (81s) | |
| PostLinks | 1.4M | 27s | |

**🏆 Performance Comparison:**

| Metric | Streaming DAG | Bulk Parallel DAG | Winner |
|--------|---------------|-------------------|--------|
| **Total Time** | **18m 14s** | 30m 26s | ✅ **Streaming (40% faster)** |
| **Posts Table** | 14m 9s | 25m 33s | ✅ **Streaming (80% faster)** |
| **Votes Table** | 9m 13s | 15m 50s | ✅ **Streaming (72% faster)** |
| **Setup Complexity** | Simple | Requires shared volume | ✅ **Streaming** |
| **Disk I/O** | Minimal | ~50GB CSV files | ✅ **Streaming** |

**Key Insights**:
- **⚡ Streaming is significantly faster**: 67% faster than bulk parallel (saves 12+ minutes)
- **Scalability**: 10x more data (12M → 106M rows) resulted in only 5x longer runtime (3.6m → 18m)
- **CSV overhead**: Bulk DAG spends significant time writing/reading CSV files to disk
- **Bottleneck**: Posts table due to large text columns (Body, Title, Tags)
- **Parallelism works**: All tables load simultaneously - runtime = slowest table, not sum
- **✅ Recommendation**: **Use Streaming DAG for ALL use cases** - faster, simpler, more efficient

### 15.3 Tested Configurations

| Configuration | Status | Notes |
|---------------|--------|-------|
| macOS ARM64 (M1/M2/M3) | ✅ Working | SQL Server in emulation mode |
| macOS AMD64 (Intel) | ✅ Working | Native performance |
| Linux AMD64 | ✅ Working | Best performance |
| Windows AMD64 | ✅ Working | Native SQL Server |

### 15.3 Optimization Tips

1. **For streaming DAG**:
   - Increase buffer size for larger rows: `SPOOLED_MAX_MEMORY_BYTES = 512 * 1024 * 1024`
   - Adjust parallelism in `.astro/config.yaml`

2. **For bulk parallel DAG**:
   - Tune partition size: `PARTITION_SIZE = 500_000` (default)
   - Use SSD storage for shared volume
   - Increase PostgreSQL `max_connections` if needed

3. **General**:
   - Allocate 8GB+ RAM to Docker Desktop
   - Use local SSD for best I/O performance
   - Monitor with `docker stats` during runs

---

## 16. License and Attribution

## 16. License and Attribution

### 16.1 Stack Overflow Database

The StackOverflow databases are provided under **CC-BY-SA 3.0** license:
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

### 17.1 Documentation

- **Project Documentation**:
  - `TESTING_RESULTS.md` - Comprehensive test results and validation
  - `CLAUDE.md` - AI assistant instructions and project context
  - `docs/optimization-summary.md` - Performance optimization analysis
  - `docs/parallel-dag-performance-analysis.md` - Parallel execution benchmarks
  - `docs/streaming-performance-analysis.md` - Streaming strategy analysis
  - `docs/troubleshooting-infrastructure.md` - Infrastructure troubleshooting guide

### 17.2 External Resources

- **Brent Ozar's Blog**: https://www.brentozar.com/archive/category/tools/stack-overflow-database/
- **Stack Exchange Data Dump**: https://archive.org/details/stackexchange
- **Apache Airflow Docs**: https://airflow.apache.org/docs/
- **Astronomer Docs**: https://docs.astronomer.io/
- **PostgreSQL Documentation**: https://www.postgresql.org/docs/
- **pymssql Documentation**: https://pymssql.readthedocs.io/
- **pg8000 Documentation**: https://github.com/tlocke/pg8000

---

## 18. Next Steps and Enhancements

### 18.1 Production Readiness

1. **Add Incremental Loads**: Modify DAG to support CDC or timestamp-based updates
2. **Add Data Quality Checks**: Implement Great Expectations or custom validation
3. **Schedule Regular Syncs**: Configure DAG `schedule_interval` for automated runs
4. **Add Alerting**: Configure Airflow email/Slack alerts for failures
5. **Add Monitoring**: Integrate with Prometheus, Grafana, or Datadog

### 18.2 Performance Enhancements

1. **Optimize Partition Sizes**: Tune `PARTITION_SIZE` based on your hardware
2. **Parallel Extraction**: Modify bulk DAG to extract partitions in parallel
3. **Compression**: Add gzip compression for CSV files to reduce I/O
4. **Connection Pooling**: Configure PostgreSQL connection pooling for better concurrency

### 18.3 Alternative Architectures

1. **PostgreSQL → PostgreSQL**: Native ARM64 support, no SQL Server emulation
2. **S3-Based Staging**: Use S3 as intermediate storage for cloud deployments
3. **Parquet Format**: Use Apache Parquet for more efficient columnar storage
4. **Delta Lake Integration**: Add incremental load capabilities with change tracking

### 18.4 Advanced Features

1. **Schema Evolution**: Handle schema changes automatically
2. **Data Validation**: Add row-level validation and data quality metrics
3. **Partitioned Tables**: Use PostgreSQL table partitioning for large tables
4. **Indexes**: Add appropriate indexes after bulk loading
5. **Foreign Keys**: Optionally restore foreign key constraints

---

## 19. Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

### Areas for Contribution

- Additional database source/target combinations (MySQL, Oracle, etc.)
- Performance optimizations for specific scenarios
- Enhanced error handling and retry logic
- Monitoring and observability integrations
- Documentation improvements

---

**Questions or Issues?** Check the troubleshooting section (Section 12) or review the comprehensive test results in `TESTING_RESULTS.md`.

**For detailed performance analysis**, see the `docs/` directory for optimization strategies and benchmarks.
