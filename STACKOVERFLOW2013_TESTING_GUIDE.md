# StackOverflow2013 Performance Testing Guide

This guide walks you through testing both DAGs with StackOverflow2013 to document accurate performance benchmarks.

## Prerequisites

- [ ] Astro environment running (`astro dev start`)
- [ ] SQL Server source container running
- [ ] PostgreSQL target container running
- [ ] StackOverflow2013 database files extracted (52GB total)

## Option 1: Automated Test (Recommended)

Run the automated test script:

```bash
./test_stackoverflow2013_performance.sh
```

This script will:
1. Attach StackOverflow2013 database
2. Update Airflow connections
3. Run streaming DAG and measure time
4. Run bulk parallel DAG and measure time
5. Save results to `performance_results.txt`

## Option 2: Manual Testing

### Step 1: Attach StackOverflow2013 Database

```bash
# Copy database files
docker exec stackoverflow-mssql-source mkdir -p /var/opt/mssql/data

docker cp include/stackoverflow/StackOverflow2013_1.mdf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2013_2.ndf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2013_3.ndf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2013_4.ndf stackoverflow-mssql-source:/var/opt/mssql/data/
docker cp include/stackoverflow/StackOverflow2013_log.ldf stackoverflow-mssql-source:/var/opt/mssql/data/

# Fix permissions
docker exec -u root stackoverflow-mssql-source chown mssql:mssql \
  /var/opt/mssql/data/StackOverflow2013*.mdf \
  /var/opt/mssql/data/StackOverflow2013*.ndf \
  /var/opt/mssql/data/StackOverflow2013*.ldf

docker exec -u root stackoverflow-mssql-source chmod 660 \
  /var/opt/mssql/data/StackOverflow2013*.mdf \
  /var/opt/mssql/data/StackOverflow2013*.ndf \
  /var/opt/mssql/data/StackOverflow2013*.ldf

# Attach database
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
  "CREATE DATABASE StackOverflow2013 ON
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_1.mdf'),
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_2.ndf'),
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_3.ndf'),
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_4.ndf'),
   (FILENAME = '/var/opt/mssql/data/StackOverflow2013_log.ldf')
   FOR ATTACH"
```

### Step 2: Get Row Counts

```bash
docker exec stackoverflow-mssql-source /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P StackOverflow123! -C -Q \
  "USE StackOverflow2013;
   SELECT 'Total Rows' AS Info,
          SUM(p.rows) AS RowCount
   FROM sys.tables t
   INNER JOIN sys.partitions p ON t.object_id = p.object_id
   WHERE p.index_id IN (0,1)
     AND t.name IN ('Users', 'Posts', 'Comments', 'Votes', 'Badges', 'PostLinks', 'VoteTypes', 'PostTypes', 'LinkTypes')"
```

Expected: ~70M+ rows

### Step 3: Update Airflow Connection

```bash
astro dev run connections delete stackoverflow_source
astro dev run connections add stackoverflow_source \
  --conn-type mssql \
  --conn-host stackoverflow-mssql-source \
  --conn-port 1433 \
  --conn-login sa \
  --conn-password "StackOverflow123!" \
  --conn-schema StackOverflow2013
```

### Step 4: Test Streaming DAG

```bash
# Unpause DAG
astro dev run dags unpause replicate_stackoverflow_to_postgres_parallel

# Record start time
echo "Start: $(date '+%Y-%m-%d %H:%M:%S')"

# Trigger DAG
astro dev run dags trigger replicate_stackoverflow_to_postgres_parallel

# Go to http://localhost:8080 and monitor the DAG
# Note the completion time from the Airflow UI
```

**Record these metrics:**
- Start time: _______________
- End time: _______________
- Total duration: _______________
- Any failures: _______________

### Step 5: Verify Data

```bash
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT
     'Users' AS table_name, COUNT(*) AS row_count FROM \"Users\"
   UNION ALL SELECT 'Posts', COUNT(*) FROM \"Posts\"
   UNION ALL SELECT 'Comments', COUNT(*) FROM \"Comments\"
   UNION ALL SELECT 'Votes', COUNT(*) FROM \"Votes\"
   UNION ALL SELECT 'Badges', COUNT(*) FROM \"Badges\"
   UNION ALL SELECT 'PostLinks', COUNT(*) FROM \"PostLinks\"
   ORDER BY table_name;"
```

### Step 6: Test Bulk Parallel DAG

```bash
# Unpause DAG
astro dev run dags unpause replicate_stackoverflow_to_postgres_bulk_parallel

# Record start time
echo "Start: $(date '+%Y-%m-%d %H:%M:%S')"

# Trigger DAG
astro dev run dags trigger replicate_stackoverflow_to_postgres_bulk_parallel

# Go to http://localhost:8080 and monitor the DAG
# Note the completion time from the Airflow UI
```

**Record these metrics:**
- Start time: _______________
- End time: _______________
- Total duration: _______________
- Extraction phase: _______________
- Loading phase: _______________
- Any failures: _______________

### Step 7: Verify Data

```bash
docker exec stackoverflow-postgres-target psql -U postgres -d stackoverflow_target -c \
  "SELECT
     'Users' AS table_name, COUNT(*) AS row_count FROM \"Users\"
   UNION ALL SELECT 'Posts', COUNT(*) FROM \"Posts\"
   UNION ALL SELECT 'Comments', COUNT(*) FROM \"Comments\"
   UNION ALL SELECT 'Votes', COUNT(*) FROM \"Votes\"
   UNION ALL SELECT 'Badges', COUNT(*) FROM \"Badges\"
   UNION ALL SELECT 'PostLinks', COUNT(*) FROM \"PostLinks\"
   ORDER BY table_name;"
```

## Performance Data Template

Copy this template and fill in your results:

```markdown
## StackOverflow2013 Performance Results

**Test Environment:**
- Date: _______________
- Hardware: _______________
- Docker Memory: _______________GB
- Airflow Workers: _______________

**Dataset:**
- Total rows: ~70M
- Database size: 52GB (4 data files + 1 log file)

**Streaming DAG (`replicate_stackoverflow_to_postgres_parallel`):**
- Total runtime: _______________
- Memory usage: _______________
- Success: ✅ / ❌
- Notes: _______________

**Bulk Parallel DAG (`replicate_stackoverflow_to_postgres_bulk_parallel`):**
- Total runtime: _______________
- Extraction phase: _______________
- Loading phase: _______________
- Disk usage: _______________GB CSV files
- Success: ✅ / ❌
- Notes: _______________

**Per-Table Breakdown:**
| Table | Rows | Streaming Time | Bulk Time |
|-------|------|----------------|-----------|
| VoteTypes | _____ | _____ | _____ |
| PostTypes | _____ | _____ | _____ |
| LinkTypes | _____ | _____ | _____ |
| Users | _____ | _____ | _____ |
| Badges | _____ | _____ | _____ |
| Posts | _____ | _____ | _____ |
| PostLinks | _____ | _____ | _____ |
| Comments | _____ | _____ | _____ |
| Votes | _____ | _____ | _____ |

**Conclusion:**
_______________
```

## Tips for Accurate Timing

1. **Use Airflow UI**: The most accurate timing is in the DAG run view
   - Click on the DAG run
   - Note the "Started" and "Ended" times
   - Duration is calculated automatically

2. **Monitor Resource Usage**:
   ```bash
   docker stats stackoverflow-mssql-source stackoverflow-postgres-target
   ```

3. **Check for Errors**:
   ```bash
   # View Airflow logs
   docker logs -f $(docker ps | grep scheduler | awk '{print $1}')
   ```

4. **Clean Between Tests**:
   - PostgreSQL target gets cleaned automatically by DAGs
   - Ensure no other processes are running

## Troubleshooting

### Issue: Database attach fails
**Solution**: Ensure files are in correct location and permissions are set
```bash
docker exec stackoverflow-mssql-source ls -lh /var/opt/mssql/data/StackOverflow2013*
```

### Issue: Out of memory
**Solution**: Increase Docker memory allocation to 16GB+
- Docker Desktop → Settings → Resources → Memory

### Issue: DAG fails on large table
**Solution**: Check Airflow worker logs for specific errors
```bash
astro dev run tasks test replicate_stackoverflow_to_postgres_parallel copy_Votes 2025-01-01
```

### Issue: Slow performance
**Causes**:
- Running on ARM64 with SQL Server emulation
- Insufficient Docker resources
- Other processes consuming resources

**Solutions**:
- Run on AMD64 hardware for best performance
- Allocate 16GB+ RAM to Docker
- Close other applications

---

## Expected Results (Estimated)

Based on StackOverflow2010 results (10M rows → 3-4 min), estimated times for StackOverflow2013 (70M rows):

- **Streaming DAG**: 20-30 minutes (7x more data)
- **Bulk Parallel DAG**: 20-30 minutes (similar to streaming)

Actual results may vary based on hardware and configuration.
