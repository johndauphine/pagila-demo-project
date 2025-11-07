# SQL Server Migration Summary

## Overview
Successfully migrated the Pagila replication pipeline from **Postgres-to-Postgres** to **Postgres-to-SQL Server** architecture. The source database remains PostgreSQL, but the target is now Azure SQL Edge (SQL Server compatible).

## Infrastructure Changes

### SQL Server Container Setup
- **Container**: Azure SQL Edge (ARM64 compatible alternative to SQL Server 2022)
- **Container Name**: `pagila-mssql-target`
- **Network**: `pagila-demo-project_6126f2_airflow` (shared with Airflow)
- **Port**: 1433 (standard SQL Server port)
- **Credentials**: 
  - User: `sa`
  - Password: `PagilaPass123!`
- **Database**: `pagila_target` (created automatically by DAG)

### Airflow Connection
- **Connection ID**: `pagila_mssql`
- **Type**: `mssql`
- **Host**: `pagila-mssql-target`
- **Port**: 1433
- **Schema**: `master` (initial connection point; DAG switches to `pagila_target`)
- **Login**: `sa`
- **Password**: `PagilaPass123!`

## Code Changes

### 1. Dependencies (`requirements.txt`)
**Added**:
```
apache-airflow-providers-microsoft-mssql
pymssql
```

### 2. Schema Conversion (`include/pagila/schema_mssql.sql`)
**Created new T-SQL schema file** with the following conversions:

| Postgres Feature | SQL Server Equivalent |
|-----------------|----------------------|
| `SERIAL` / `BIGSERIAL` | `INT IDENTITY(1,1)` |
| `TIMESTAMP` | `DATETIME2` |
| `TEXT` | `NVARCHAR(MAX)` |
| `VARCHAR(n)` | `NVARCHAR(n)` |
| `BYTEA` | `VARBINARY(MAX)` |
| `BOOLEAN` | `BIT` |
| `NOW()` | `GETDATE()` |

**Removed Postgres-specific features**:
- `DOMAIN` types (converted to base types with constraints)
- `ENUM` types (converted to `NVARCHAR` with check constraints or removed)
- `pl/pgsql` functions (trigger functions removed; identity handles auto-increment)
- `OWNER` statements (not applicable in SQL Server)
- `GO` batch separators (not supported by pymssql)

**Schema simplifications**:
- 15 core tables: language, category, actor, film, film_actor, film_category, country, city, address, store, customer, staff, inventory, rental, payment
- Foreign key constraints preserved
- Indexes preserved where applicable
- Circular reference between `store` and `staff` handled with `ALTER TABLE` after both exist

### 3. DAG Refactoring (`dags/replicate_pagila_to_target.py`)

#### Import Changes
```python
# Added
import io
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# Removed
from io import StringIO  # Replaced with io module
```

#### Connection Configuration
```python
TGT_CONN_ID = "pagila_mssql"  # Changed from "pagila_tgt"
ENSURE_POSTGRES_ROLE_ON_TARGET = False  # SQL Server doesn't use Postgres roles
```

#### Function: `reset_target_schema()`
**Changes**:
- Uses `MsSqlHook` instead of second `PostgresHook`
- Creates `pagila_target` database if it doesn't exist
- Drops foreign key constraints first (SQL Server requirement)
- Drops tables in `dbo` schema
- Uses `conn.autocommit(True)` function call (pymssql constraint)
- Manually manages cursor and connection lifecycle

**Key SQL differences**:
```sql
-- Database creation
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'pagila_target')
BEGIN
    CREATE DATABASE pagila_target;
END

-- Drop foreign keys first
SELECT @sql += 'ALTER TABLE ... DROP CONSTRAINT ...'
FROM sys.foreign_keys;

-- Drop tables
SELECT @sql += 'DROP TABLE [dbo].[' + name + '];'
FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo');
```

#### Function: `ensure_tgt_roles()`
**Changes**:
- Converted to no-op function
- SQL Server uses different security model (no Postgres roles)

#### Function: `copy_table_src_to_tgt()`
**Major rewrite** for CSV-based bulk insert:

**Data Flow**:
1. **Extract from Postgres**: `COPY TO STDOUT` with CSV format
2. **Buffer**: `SpooledTemporaryFile` in binary mode (`w+b`)
3. **Transform**: Wrap with `io.TextIOWrapper` for CSV reading
4. **Load to SQL Server**: Batch INSERT statements (1000 rows per batch)

**Key changes**:
```python
# Binary mode for SpooledTemporaryFile (COPY outputs bytes)
with SpooledTemporaryFile(max_size=SPOOLED_MAX_MEMORY_BYTES, mode="w+b") as spool:
    # ... COPY data to spool ...
    
    # Wrap in text mode for CSV parsing
    text_spool = io.TextIOWrapper(spool, encoding='utf-8', newline='')
    
    # Switch to pagila_target database
    tgt_cur.execute("USE pagila_target;")
    
    # DELETE instead of TRUNCATE (foreign key compatibility)
    tgt_cur.execute(f"DELETE FROM dbo.[{table}]")
    
    # Batch insert with NULL handling
    csv_reader = csv.reader(text_spool)
    for row in csv_reader:
        processed_row = [None if v == "" else v for v in row]
        batch.append(processed_row)
        
        if len(batch) >= 1000:
            # INSERT INTO ... VALUES (%s, %s, ...), (%s, %s, ...), ...
```

**SQL Server-specific adjustments**:
- Placeholder: `%s` (pymssql convention) instead of `?`
- DELETE instead of TRUNCATE (foreign key constraints prevent TRUNCATE)
- Explicit `USE pagila_target;` before queries
- Manual transaction management with `conn.autocommit(False)` and `conn.commit()`

#### Function: `set_identity_sequences()`
**Changes**:
```python
# Postgres
cur.execute(f"SELECT setval('public.{table}_{pk}_seq', {max_id});")

# SQL Server  
cur.execute(f"DBCC CHECKIDENT ('dbo.{table}', RESEED, {max_id})")
```

**Added tables**:
- `("language", "language_id")` to identity_tables list

#### DAG Tags
```python
tags=["pagila", "postgres", "replication", "mssql"]  # Added "mssql"
```

## Technical Challenges & Solutions

### Challenge 1: GO Batch Separator
**Error**: `pymssql._mssql.MSSQLDatabaseException: (102, b"Incorrect syntax near 'GO'.`

**Solution**: Removed `GO` statements from `schema_mssql.sql`. The `GO` keyword is a SQL Server Management Studio convention, not valid T-SQL for pymssql.

### Challenge 2: Autocommit Attribute
**Error**: `AttributeError: 'pymssql._pymssql.Connection' object attribute 'autocommit' is read-only`

**Solution**: Changed from `conn.autocommit = True` to `conn.autocommit(True)` (function call).

### Challenge 3: Binary vs Text Mode
**Error**: `TypeError: write() argument must be str, not bytes`

**Solution**: 
- Use binary mode for `SpooledTemporaryFile` (Postgres COPY outputs bytes)
- Wrap with `io.TextIOWrapper` for CSV reading (text mode)

### Challenge 4: TRUNCATE with Foreign Keys
**Error**: `pymssql.exceptions.OperationalError: (4712, b"Cannot truncate table 'dbo.language' because it is being referenced by a FOREIGN KEY constraint.`

**Solution**: Use `DELETE FROM` instead of `TRUNCATE TABLE`.

### Challenge 5: Database Connection
**Error**: `Login failed for user 'sa'. Reason: Failed to open the explicitly specified database 'pagila_target'.`

**Solution**: 
- Connect to `master` database initially
- DAG creates `pagila_target` database if it doesn't exist
- Use `USE pagila_target;` to switch context in each function

### Challenge 6: IDENTITY_INSERT Error on film Table
**Error**: `(544, b"Cannot insert explicit value for identity column when IDENTITY_INSERT is set to OFF")`

**Solution**: Added `SET IDENTITY_INSERT dbo.[table] ON` before inserts, then `OFF` after completion.

### Challenge 7: Datetime Timezone Conversion
**Error**: `(241, b'Conversion failed when converting date and/or time from character string')`

**Root Cause**: Postgres timestamps include timezone info (`2022-02-15 10:02:19+00`), but SQL Server DATETIME2 doesn't accept this format.

**Solution**: Strip timezone during CSV processing:
```python
if "+" in v and " " in v:  # Timestamp with timezone
    clean_ts = v.split("+")[0]  # "2022-02-15 10:02:19"
    processed_row.append(clean_ts)
```

### Challenge 8: Column Count Mismatch on film Table
**Error**: `(110, b'There are fewer columns in the INSERT statement than values specified in the VALUES clause')`

**Root Cause**: Postgres `film` table has `fulltext` column (tsvector), but SQL Server schema omits it.

**Solution**: Query target columns first, only SELECT matching columns from Postgres:
```python
# Get target columns
tgt_cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=%s ORDER BY ORDINAL_POSITION", (table,))
target_columns = [row[0] for row in tgt_cur.fetchall()]

# Build column list for COPY
column_list_for_select = ", ".join(f'"{col}"' for col in target_columns)
copy_sql = f"COPY (SELECT {column_list_for_select} FROM public.{table}) TO STDOUT WITH CSV"
```

### Challenge 9: IDENTITY_INSERT Error on Junction Tables
**Error**: `(8106, b"Table 'dbo.film_actor' does not have the identity property. Cannot perform SET operation")`

**Root Cause**: Junction tables (film_actor, film_category) have composite primary keys, no identity column.

**Solution**: Check if table has identity column before enabling IDENTITY_INSERT:
```python
tgt_cur.execute("""
    SELECT COUNT(*)
    FROM sys.identity_columns ic
    INNER JOIN sys.tables t ON ic.object_id = t.object_id
    WHERE t.name = %s
""", (table,))
has_identity = tgt_cur.fetchone()[0] > 0

if has_identity:
    tgt_cur.execute(f"SET IDENTITY_INSERT dbo.[{table}] ON")
```

### Challenge 10: Empty String vs NULL in NOT NULL Columns
**Error**: `(515, b"Cannot insert the value NULL into column 'phone', table 'pagila_target.dbo.address'; column does not allow nulls")`

**Root Cause**: CSV can't distinguish between empty string `""` and NULL. Postgres allows empty strings in `phone TEXT NOT NULL`, but our code converted all empty strings to NULL.

**Solution**: Query column nullability, only convert empty strings to NULL for nullable columns:
```python
# Get nullable columns
tgt_cur.execute("SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=%s", (table,))
column_info = tgt_cur.fetchall()
nullable_columns = {row[0] for row in column_info if row[1] == 'YES'}

# Process row data
if v == "":
    if col_name in nullable_columns:
        processed_row.append(None)  # NULL for nullable columns
    else:
        processed_row.append("")    # Empty string for NOT NULL columns
```

### Challenge 11: Boolean Conversion
**Error**: `(245, b"Conversion failed when converting the nvarchar value 't' to data type bit")`

**Root Cause**: Postgres BOOLEAN outputs `'t'`/`'f'` in CSV format, but SQL Server BIT expects `'1'`/`'0'`.

**Solution**: Convert boolean values during row processing:
```python
elif v == "t":  # Postgres boolean true
    processed_row.append("1")
elif v == "f":  # Postgres boolean false
    processed_row.append("0")
```

### Challenge 12: Circular Foreign Key Dependency
**Error**: `(547, b'The INSERT statement conflicted with the FOREIGN KEY constraint "fk_store_staff"')`

**Root Cause**: Circular dependency between `store` and `staff`:
- `store.manager_staff_id` → `staff.staff_id`
- `staff.store_id` → `store.store_id`

**Solution**: Disable FK constraints during load, re-enable after:
```python
# Before insert
tgt_cur.execute(f"ALTER TABLE dbo.[{table}] NOCHECK CONSTRAINT ALL")

# ... INSERT data ...

# After insert
tgt_cur.execute(f"ALTER TABLE dbo.[{table}] CHECK CONSTRAINT ALL")
```

### Challenge 13: VARCHAR Size Truncation
**Error**: `(2628, b"String or binary data would be truncated in table 'pagila_target.dbo.staff', column 'username'")`

**Root Cause**: Postgres `username TEXT` converted to SQL Server `NVARCHAR(16)`, but data contains usernames like "natisha.pfanners" (17 chars).

**Solution**: Increased column size in `schema_mssql.sql`:
```sql
[username] NVARCHAR(50) NOT NULL,  -- Was NVARCHAR(16)
```

### Challenge 14: Schema Mismatch on payment Table
**Error**: `psycopg2.errors.UndefinedColumn: column "last_update" does not exist`

**Root Cause**: SQL Server schema included `last_update` column, but Postgres `payment` table is partitioned and doesn't have this column.

**Solution**: Removed `last_update` from SQL Server payment table definition to match Postgres schema.

## Performance Characteristics

### Memory Management
- **Memory cap**: 128 MB (`SPOOLED_MAX_MEMORY_BYTES`)
- **Spill behavior**: Automatically writes to disk when buffer exceeds 128 MB
- **Observed for `language` table**: 276 bytes buffered, no disk spill

### Batch Processing
- **Batch size**: 1000 rows per INSERT statement
- **NULL handling**: Empty CSV strings converted to SQL NULL (only for nullable columns)
- **Boolean conversion**: Postgres `'t'`/`'f'` → SQL Server `'1'`/`'0'`
- **Datetime conversion**: Strip timezone info (`+00`, `Z`) for DATETIME2 compatibility
- **Transaction**: Single transaction per table for consistency

### Network Efficiency
- **Source → Buffer**: Single COPY operation (Postgres native protocol)
- **Buffer → Target**: Batched INSERT reduces round trips
- **Encoding**: UTF-8 throughout pipeline

### Data Validation
- **Column matching**: Query target schema first, only copy matching columns from source
- **Identity detection**: Check `sys.identity_columns` before enabling IDENTITY_INSERT
- **Nullability check**: Query `IS_NULLABLE` to handle empty strings correctly
- **FK constraints**: Disabled during load to handle circular dependencies, re-enabled after

## Test Results

### Successful Full Pipeline Test
**Command**: `astro dev run dags test replicate_pagila_to_target 2024-12-08`

**Results**:
- ✅ **DAG State**: SUCCESS
- ✅ **Runtime**: ~29 seconds
- ✅ **Tables Replicated**: 15/15
- ✅ **Sequence Alignment**: Completed for all identity columns

**Table Load Sequence**:
1. language (6 rows)
2. category (16 rows)
3. actor (200 rows)
4. film (1000 rows)
5. film_actor (5462 rows)
6. film_category (1000 rows)
7. country (109 rows)
8. city (600 rows)
9. address (603 rows)
10. staff (2 rows)
11. store (2 rows)
12. customer (599 rows)
13. inventory (4581 rows)
14. rental (16044 rows)
15. payment (16049 rows)
16. align_target_sequences (identity reset)

**Total Rows**: ~45,000+ rows successfully replicated

## Testing & Validation

### Prerequisites
1. **Airflow Metadata Reset** (if needed):
```bash
astro dev run db reset -y
astro dev restart
```

2. **Recreate Connections**:
```bash
# SQL Server target
astro dev run connections add pagila_mssql \
  --conn-type mssql \
  --conn-host pagila-mssql-target \
  --conn-login sa \
  --conn-password 'PagilaPass123!' \
  --conn-port 1433 \
  --conn-schema master

# Postgres source
astro dev run connections add pagila_postgres \
  --conn-type postgres \
  --conn-host pagila-pg-source \
  --conn-login pagila \
  --conn-password pagila_pw \
  --conn-port 5432 \
  --conn-schema pagila
```

### Test Command
```bash
astro dev run dags test replicate_pagila_to_target 2024-11-14
```

### Expected Output
```
[language] starting buffered copy (memory cap≈128.0 MB)
[language] buffered 276 bytes (rolled_to_disk=False)
[language] copy completed (bytes=276, rows=6)
...
DagRun Finished: state=success
```

### Validation Queries
```sql
-- Connect to SQL Server
docker exec -it pagila-mssql-target /bin/bash

-- Verify row counts in SQL Server
USE pagila_target;

SELECT 'language' AS table_name, COUNT(*) AS row_count FROM dbo.language
UNION ALL SELECT 'category', COUNT(*) FROM dbo.category
UNION ALL SELECT 'actor', COUNT(*) FROM dbo.actor
UNION ALL SELECT 'film', COUNT(*) FROM dbo.film
UNION ALL SELECT 'customer', COUNT(*) FROM dbo.customer
UNION ALL SELECT 'rental', COUNT(*) FROM dbo.rental
UNION ALL SELECT 'payment', COUNT(*) FROM dbo.payment
ORDER BY table_name;

-- Verify data integrity
SELECT TOP 5 * FROM dbo.film;
SELECT TOP 5 * FROM dbo.customer;

-- Verify identity sequences
SELECT 
    OBJECT_NAME(object_id) AS table_name,
    IDENT_CURRENT(OBJECT_NAME(object_id)) AS current_identity
FROM sys.identity_columns
WHERE OBJECT_SCHEMA_NAME(object_id) = 'dbo'
ORDER BY table_name;
```

## Files Modified

1. **`requirements.txt`**: Added MSSQL provider packages
2. **`dags/replicate_pagila_to_target.py`**: Complete refactor for SQL Server
3. **`include/pagila/schema_mssql.sql`**: New T-SQL schema (created)
4. **`README.md`**: Updated with SpooledTemporaryFile documentation

## Files Created (Temporary)
- `create_mssql_db.py`: Helper script (not used in final solution)
- `setup_mssql_db.py`: Helper script (not used in final solution)

## Migration Checklist

- [x] Install SQL Server provider packages
- [x] Create T-SQL schema with proper type conversions
- [x] Set up Azure SQL Edge container
- [x] Create Airflow connections (pagila_mssql, pagila_postgres)
- [x] Refactor `reset_target_schema()` for SQL Server
- [x] Refactor `copy_table_src_to_tgt()` for CSV bulk insert
- [x] Refactor `set_identity_sequences()` using DBCC CHECKIDENT
- [x] Update DAG tags and metadata
- [x] Fix GO statement issue
- [x] Fix autocommit API
- [x] Fix binary/text mode handling
- [x] Fix TRUNCATE → DELETE
- [x] Add IDENTITY_INSERT enable/disable logic
- [x] Add datetime timezone stripping
- [x] Add column matching between source and target
- [x] Add conditional IDENTITY_INSERT (check sys.identity_columns)
- [x] Add nullable column detection for empty string handling
- [x] Add boolean conversion (t/f → 1/0)
- [x] Add FK constraint disable/enable for circular dependencies
- [x] Fix username column size (16 → 50 chars)
- [x] Remove last_update from payment table
- [x] Test end-to-end replication (SUCCESS)
- [x] Validate all 15 tables replicated
- [x] Validate identity sequence alignment

## Next Steps (Optional Enhancements)

1. **Performance Optimization**:
   - Consider using SQL Server BULK INSERT with temp files
   - Adjust batch size based on row size
   - Disable indexes during load, rebuild after

2. **Schema Enhancements**:
   - Add missing Postgres features (functions, views)
   - Implement triggers if needed
   - Add SQL Server-specific indexes (columnstore, etc.)

3. **Monitoring**:
   - Add row count validation after each table copy
   - Log performance metrics (rows/sec, MB/sec)
   - Alert on data mismatch

4. **Error Handling**:
   - Retry logic for transient SQL Server errors
   - Deadlock detection and handling
   - Connection pool management

## References

- [Apache Airflow MSSQL Provider](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/)
- [pymssql Documentation](https://pymssql.readthedocs.io/)
- [Azure SQL Edge](https://learn.microsoft.com/en-us/azure/azure-sql-edge/overview)
- [SQL Server IDENTITY](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property)
- [DBCC CHECKIDENT](https://learn.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-checkident-transact-sql)
