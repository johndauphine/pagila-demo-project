<!-- Git commit message: Remove advisory lock tasks and enforce shared default retries -->
# Pagila End-to-End Replication Pipeline Using Astro + Apache Airflow
## 1. Introduction
This document provides a complete, production-quality guide to building an end-to-end data replication pipeline using Postgres (source + target), Astronomer (Astro CLI), Apache Airflow 3, and Python-based ETL using COPY streaming. It includes fully working DAGs with idempotency, schema reset, partition-safe replication, advisory locks, and simple auditing.

---

## 2. Prerequisites
- Docker Desktop
- Astro CLI
- Python 3.10+
- Add these to your Astro project's `requirements.txt`:
  - `apache-airflow-providers-postgres`
  - `apache-airflow-providers-common-sql`
  - `psycopg2-binary`

Then rebuild/restart your Astro environment:

```bash
astro dev restart
```
 
## 3. Start Source and Target Database Containers

### 3.1 Start Source Postgres Container
Start **source** on host port **5433**:
```bash
docker run -d \
  --name pagila-pg-source \
  -e POSTGRES_DB=pagila \
  -e POSTGRES_USER=pagila \
  -e POSTGRES_PASSWORD=pagila_pw \
  -p 5433:5432 \
  postgres:16
```

### 3.2 Start Target Database Container

You have two options for the target database:

#### Option A: Postgres Target (Default)
Start **Postgres target** on host port **5444**:
```bash
docker run -d \
  --name pagila-pg-target \
  -e POSTGRES_DB=pagila \
  -e POSTGRES_USER=pagila_tgt \
  -e POSTGRES_PASSWORD=pagila_tgt_pw \
  -p 5444:5432 \
  postgres:16
```

#### Option B: SQL Server Target (Alternative)
Start **Azure SQL Edge target** (ARM64/AMD64 compatible) on host port **1433**:
```bash
docker run -d \
  --name pagila-mssql-target \
  -e "ACCEPT_EULA=Y" \
  -e "MSSQL_SA_PASSWORD=PagilaPass123!" \
  -p 1433:1433 \
  mcr.microsoft.com/azure-sql-edge:latest
```

> **Note**: Azure SQL Edge is recommended over SQL Server 2022 for ARM64 (Apple Silicon) compatibility. For production, use full SQL Server 2022 on AMD64.

### 3.3 Connect to Astro Network

The Astro runtime creates a project-specific bridge network (for example `pagila-demo-project_xxxxx_airflow`). Attach your database containers so the scheduler can resolve them by container name:

**For Postgres target**:
```bash
# First, start Astro to create the network
astro dev start

# Discover the network name
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'pagila-demo-project.*_airflow')

# Connect containers
docker network connect $ASTRO_NETWORK pagila-pg-source
docker network connect $ASTRO_NETWORK pagila-pg-target
```

**For SQL Server target**:
```bash
# First, start Astro to create the network
astro dev start

# Discover the network name
ASTRO_NETWORK=$(docker network ls --format '{{.Name}}' | grep 'pagila-demo-project.*_airflow')

# Connect containers
docker network connect $ASTRO_NETWORK pagila-pg-source
docker network connect $ASTRO_NETWORK pagila-mssql-target
```

> To discover the exact network name manually, run `docker network ls | grep astro`. If you prefer `docker compose`, you can wrap these services in a compose file as long as it joins the same Astro network.

## 4. Create Airflow Connections

### 4.1 Source Connection (Always Required)
Create the Postgres source connection:

```bash
astro dev run connections add pagila_postgres \
  --conn-type postgres \
  --conn-host pagila-pg-source \
  --conn-port 5432 \
  --conn-login pagila \
  --conn-password pagila_pw \
  --conn-schema pagila
```

### 4.2 Target Connection (Choose One)

#### Option A: Postgres Target
```bash
astro dev run connections add pagila_tgt \
  --conn-type postgres \
  --conn-host pagila-pg-target \
  --conn-port 5432 \
  --conn-login pagila_tgt \
  --conn-password pagila_tgt_pw \
  --conn-schema pagila
```

#### Option B: SQL Server Target
```bash
astro dev run connections add pagila_mssql \
  --conn-type mssql \
  --conn-host pagila-mssql-target \
  --conn-port 1433 \
  --conn-login sa \
  --conn-password "PagilaPass123!" \
  --conn-schema master
```

> **Important**: The SQL Server connection initially connects to the `master` database. The DAG will create and switch to the `pagila_target` database automatically.

### 4.3 Additional Requirements for SQL Server

If using SQL Server as the target, add these packages to `requirements.txt`:
```
apache-airflow-providers-microsoft-mssql
pymssql
```

Then restart Astro:
```bash
astro dev restart
```

> On Docker Desktop you can still point at the mapped host ports by swapping `--conn-host` for `host.docker.internal` and using ports `5433` (Postgres source), `5444` (Postgres target), or `1433` (SQL Server target).

## 5. Place SQL Files

### 5.1 Postgres Schema Files (Required for Source and Postgres Target)
Place the Pagila SQL files in your Astro project at:
```
include/pagila/schema.sql
include/pagila/data.sql
```
Ensure `data.sql` uses `COPY ... FROM stdin;` blocks with a terminating `\.` line for each section (download a **raw** file from the official repo).

Download the canonical files straight from GitHub:

```bash
curl -fsSL https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql -o include/pagila/schema.sql
curl -fsSL https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql -o include/pagila/data.sql
```

### 5.2 SQL Server Schema File (Required for SQL Server Target)
If using SQL Server as the target, you also need the T-SQL schema file:
```
include/pagila/schema_mssql.sql
```

This file is included in the repository and contains the converted T-SQL schema with:
- `INT IDENTITY(1,1)` instead of `SERIAL`/`BIGSERIAL`
- `NVARCHAR` instead of `VARCHAR`/`TEXT`
- `DATETIME2` instead of `TIMESTAMP`
- `BIT` instead of `BOOLEAN`
- `DECIMAL` instead of `NUMERIC`
- Removed PostgreSQL-specific features (DOMAIN types, ENUM types, pl/pgsql functions)

> The SQL Server schema is maintained separately because it uses different data types and syntax than PostgreSQL. See `MSSQL_MIGRATION.md` for detailed conversion notes.

## 6. Full DAG: Idempotent Pagila Loader (Source)

**File:** `dags/load_pagila_dag.py`

```python
from __future__ import annotations

import hashlib
from datetime import datetime
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "pagila_postgres"
SCHEMA_SQL_PATH = "/usr/local/airflow/include/pagila/schema.sql"
DATA_SQL_PATH = "/usr/local/airflow/include/pagila/data.sql"
AUDIT_SCHEMA = "pagila_support"
AUDIT_TABLE = "pagila_support.pagila_load_audit"

# Toggle this off if your schema/data do not contain 'OWNER TO postgres'.
ENSURE_POSTGRES_ROLE = True


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_input_fingerprints(**context) -> None:
    schema_hash = file_sha256(SCHEMA_SQL_PATH)
    data_hash = file_sha256(DATA_SQL_PATH)
    ti = context["ti"]
    ti.xcom_push(key="schema_hash", value=schema_hash)
    ti.xcom_push(key="data_hash", value=data_hash)


def should_reload(**context) -> bool:
    ti = context["ti"]
    schema_hash = ti.xcom_pull(key="schema_hash", task_ids="compute_hashes")
    data_hash = ti.xcom_pull(key="data_hash", task_ids="compute_hashes")
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
              id                bigserial PRIMARY KEY,
              loaded_at         timestamptz NOT NULL DEFAULT now(),
              schema_sha256     text NOT NULL,
              data_sha256       text NOT NULL,
              actor_count       bigint,
              film_count        bigint,
              customer_count    bigint,
              rental_count      bigint,
              succeeded         boolean NOT NULL
            );
            """
        )
        cur.execute(
            f"""
            SELECT succeeded
            FROM {AUDIT_TABLE}
            WHERE schema_sha256=%s AND data_sha256=%s
            ORDER BY loaded_at DESC
            LIMIT 1;
            """,
            (schema_hash, data_hash),
        )
        row = cur.fetchone()
        if row and row[0] is True:
            print("Idempotence: inputs already loaded successfully. Skipping reload.")
            return False
    print("Idempotence: inputs not seen before (or last load failed). Will reload.")
    return True


def acquire_lock() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(863440123987650321);")
        (locked,) = cur.fetchone()
        if not locked:
            raise RuntimeError("Could not obtain advisory lock; another run holds it.")


def release_lock() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock_all();")


def load_pagila_copy_blocks() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    def iter_copy_sections(path: str):
        with open(path, "rb") as f:
            in_copy = False
            buf = bytearray()
            copy_sql = None
            for raw in f:
                line = raw.decode("utf-8", errors="strict")
                if not in_copy:
                    if line.startswith("COPY ") and " FROM stdin;" in line:
                        copy_sql = line.strip()
                        in_copy = True
                        buf.clear()
                else:
                    if line.strip() == "\\.":
                        with BytesIO(bytes(buf)) as fp:
                            cur.copy_expert(copy_sql, fp)
                        in_copy = False
                        buf.clear()
                        copy_sql = None
                    else:
                        buf.extend(raw)
            if in_copy:
                raise RuntimeError("Unterminated COPY block in data.sql")

    iter_copy_sections(DATA_SQL_PATH)
    cur.close()
    conn.close()


def record_success(**context) -> None:
    ti = context["ti"]
    schema_hash = ti.xcom_pull(key="schema_hash", task_ids="compute_hashes")
    data_hash = ti.xcom_pull(key="data_hash", task_ids="compute_hashes")
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
              id                bigserial PRIMARY KEY,
              loaded_at         timestamptz NOT NULL DEFAULT now(),
              schema_sha256     text NOT NULL,
              data_sha256       text NOT NULL,
              actor_count       bigint,
              film_count        bigint,
              customer_count    bigint,
              rental_count      bigint,
              succeeded         boolean NOT NULL
            );
            """
        )
        cur.execute("SELECT COUNT(*) FROM actor;")
        actor = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM film;")
        film = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM customer;")
        cust = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM rental;")
        rent = cur.fetchone()[0]
        cur.execute(
            f"""
            INSERT INTO {AUDIT_TABLE}
              (schema_sha256, data_sha256, actor_count, film_count, customer_count, rental_count, succeeded)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE);
            """,
            (schema_hash, data_hash, actor, film, cust, rent),
        )


with DAG(
    dag_id="load_pagila_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["/usr/local/airflow/include"],
    tags=["pagila", "postgres", "idempotent"],
) as dag:
    compute_hashes = PythonOperator(
        task_id="compute_hashes",
        python_callable=compute_input_fingerprints,
    )

    short_circuit = ShortCircuitOperator(
        task_id="skip_if_already_loaded",
        python_callable=should_reload,
    )

    lock = PythonOperator(
        task_id="acquire_lock",
        python_callable=acquire_lock,
    )

    reset_public_schema = SQLExecuteQueryOperator(
        task_id="reset_public_schema",
        conn_id=CONN_ID,
        sql="""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM information_schema.schemata WHERE schema_name='public'
          ) THEN
            EXECUTE 'DROP SCHEMA public CASCADE';
          END IF;
          EXECUTE 'CREATE SCHEMA public AUTHORIZATION current_user';
        END$$;
        """
    )

    ensure_role = SQLExecuteQueryOperator(
        task_id="ensure_postgres_role",
        conn_id=CONN_ID,
        sql="""
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='postgres') THEN
            CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'postgres_pw';
          END IF;
        END$$;
        """
    )

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id=CONN_ID,
        sql="pagila/schema.sql",
    )

    load_data = PythonOperator(
        task_id="load_data_streaming",
        python_callable=load_pagila_copy_blocks,
    )

    verify_counts = SQLExecuteQueryOperator(
        task_id="verify_counts",
        conn_id=CONN_ID,
        sql=[
            "SELECT COUNT(*) FROM actor;",
            "SELECT COUNT(*) FROM film;",
            "SELECT COUNT(*) FROM customer;",
            "SELECT COUNT(*) FROM rental;",
        ],
    )

    write_audit = PythonOperator(
        task_id="record_success",
        python_callable=record_success,
    )

    unlock = PythonOperator(
        task_id="release_lock",
        trigger_rule="all_done",
        python_callable=release_lock,
    )

    compute_hashes >> short_circuit
    short_circuit >> lock
    lock >> reset_public_schema
    if ENSURE_POSTGRES_ROLE:
        reset_public_schema >> ensure_role >> create_schema
    else:
        reset_public_schema >> create_schema
    create_schema >> load_data >> verify_counts >> write_audit >> unlock
```

> Audit history lives in `pagila_support.pagila_load_audit`, which is left untouched even when the DAG drops and recreates the `public` schema.
```

---

## 7. Full DAG: Pagila Replication (Source → Target)

**File:** `dags/replicate_pagila_to_target.py`

This DAG now uses a **memory-capped buffered streaming** approach powered by `tempfile.SpooledTemporaryFile`. Up to 128 MB of each table copy stays in memory; any overflow spills transparently to the worker’s disk, keeping resource usage predictable without the complexity of multi-threaded queues.

```python
from tempfile import SpooledTemporaryFile

SPOOLED_MAX_MEMORY_BYTES = 128 * 1024 * 1024  # spill threshold (~128 MB)


def copy_table_src_to_tgt(table: str) -> None:
    src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID)

    with SpooledTemporaryFile(max_size=SPOOLED_MAX_MEMORY_BYTES, mode="w+b") as spool:
        with src_hook.get_conn() as src_conn:
            src_conn.autocommit = True
            with src_conn.cursor() as src_cur:
                src_cur.copy_expert(
                    f"COPY (SELECT * FROM public.{table}) TO STDOUT",
                    spool,
                )

        spool.flush()
        written_bytes = spool.tell()
        spilled_to_disk = bool(getattr(spool, "_rolled", False))
        log.info(
            "[%s] buffered %s bytes (rolled_to_disk=%s)",
            table,
            written_bytes,
            spilled_to_disk,
        )

        spool.seek(0)
        with tgt_hook.get_conn() as tgt_conn:
            tgt_conn.autocommit = True
            with tgt_conn.cursor() as tgt_cur:
                tgt_cur.copy_expert(
                    f"COPY public.{table} FROM STDIN",
                    spool,
                )
```

**Key Features:**
- **Predictable Resource Use**: Keeps <128 MB in RAM, spills larger tables automatically
- **Straightforward Flow**: Single process handles read → write without queue plumbing
- **Visibility**: Log output records bytes copied and whether disk spill occurred
- **Production-Ready**: Validated end-to-end with Pagila (language=6, rental=16 044, payment=16 049 rows)

> Adjust `SPOOLED_MAX_MEMORY_BYTES` if your worker can spare more or less RAM before spilling; the rest of the DAG (task layout, retries, sequence fixes) stays the same.

---

## 8. Running the DAGs
```bash
astro dev start

# Unpause the DAGs once they appear
astro dev run dags unpause -y load_pagila_to_postgres
astro dev run dags unpause -y replicate_pagila_to_target

# Load source
astro dev run dags trigger load_pagila_to_postgres

# Replicate to target
astro dev run dags trigger replicate_pagila_to_target
```

---

## 9. Verification & QA

### 9.1 Postgres Target Verification
```bash
# Row counts on target (examples)
docker exec -it pagila-pg-target psql -U pagila_tgt -d pagila -c "SELECT COUNT(*) FROM actor;"
docker exec -it pagila-pg-target psql -U pagila_tgt -d pagila -c "SELECT COUNT(*) FROM rental;"

# Audit ledger on source
docker exec -it pagila-pg-source psql -U pagila -d pagila -c "SELECT loaded_at, schema_sha256, data_sha256, succeeded FROM pagila_support.pagila_load_audit ORDER BY loaded_at DESC LIMIT 5;"
```

### 9.2 SQL Server Target Verification
```bash
# Connect to SQL Server and check row counts
docker exec -it pagila-mssql-target /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "PagilaPass123!" -Q "USE pagila_target; SELECT 'actor' AS table_name, COUNT(*) AS row_count FROM dbo.actor;"

docker exec -it pagila-mssql-target /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "PagilaPass123!" -Q "USE pagila_target; SELECT 'rental' AS table_name, COUNT(*) AS row_count FROM dbo.rental;"

# Comprehensive row count check (all tables)
docker exec -it pagila-mssql-target /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "PagilaPass123!" -Q "USE pagila_target; SELECT 'language' AS t, COUNT(*) AS c FROM dbo.language UNION ALL SELECT 'actor', COUNT(*) FROM dbo.actor UNION ALL SELECT 'film', COUNT(*) FROM dbo.film UNION ALL SELECT 'rental', COUNT(*) FROM dbo.rental UNION ALL SELECT 'payment', COUNT(*) FROM dbo.payment;"
```

### Expected Row Counts (Both Targets)
| Table | Rows |
|-------|------|
| language | 6 |
| category | 16 |
| actor | 200 |
| film | 1,000 |
| film_actor | 5,462 |
| film_category | 1,000 |
| country | 109 |
| city | 600 |
| address | 605 |
| store | 2 |
| customer | 599 |
| staff | 2 |
| inventory | 4,581 |
| rental | 16,044 |
| payment | 16,049 |

For a quick smoke test, counts on target should match source. The audit query on source confirms the loader recognized your latest schema + data hashes.

---

## 10. Troubleshooting
- **WrongObjectType** on partitioned tables: fixed by using `COPY (SELECT ...)` for extraction.
- **Connection not found**: verify with `astro dev run connections list`; ensure names match DAG constants.
- **Port conflicts**: change host ports (5433/5444) when starting containers.
- **Unicode escapes**: do not include `\N` inside Python strings; keep COPY payload only in `.sql` files.
- **Owner mismatch**: if your schema uses `OWNER TO postgres`, either create that role (see DAG) or replace owners in the SQL files.

---

## 11. File Layout (Astro project)
```
.
├── dags
│   ├── load_pagila_dag.py
│   └── replicate_pagila_to_target.py
├── include
│   └── pagila
│       ├── schema.sql          # Postgres schema (source + Postgres target)
│       ├── data.sql            # Postgres data (source only)
│       └── schema_mssql.sql    # SQL Server schema (SQL Server target only)
├── requirements.txt
├── MSSQL_MIGRATION.md          # SQL Server migration guide
├── AGENTS.md                   # Repository guidelines for contributors
└── .env   (optional for AIRFLOW_CONN_... variables)
```

---

## 12. DAG Concurrency & Retries
- Both `load_pagila_to_postgres` and `replicate_pagila_to_target` now share a `default_args` block that enforces at least two retries with a one-minute backoff. This keeps the DAGs compliant with the pytest guardrails in `tests/dags/test_dag_example.py`.
- Advisory locks were removed from the loader DAG. If you need multi-run serialization in the future, rely on Airflow’s built-in `max_active_runs=1` or reintroduce a session-scoped lock that persists across tasks.
- When running the pipeline, avoid triggering overlapping DAG runs unless you intentionally want concurrent replays.

---

## 13. SQL Server Target Configuration

### 13.1 Overview
The `replicate_pagila_to_target` DAG supports both **PostgreSQL** and **SQL Server** as target databases. The SQL Server implementation uses CSV-based bulk loading with memory-efficient streaming.

### 13.2 Architecture Differences

| Aspect | Postgres Target | SQL Server Target |
|--------|----------------|-------------------|
| **Hook** | `PostgresHook` | `MsSqlHook` |
| **Connection ID** | `pagila_tgt` | `pagila_mssql` |
| **Schema File** | `schema.sql` | `schema_mssql.sql` |
| **Data Transfer** | `COPY TO/FROM` | CSV streaming + INSERT |
| **Truncate** | `TRUNCATE TABLE` | `DELETE FROM` (FK compat) |
| **Sequences** | `setval()` | `DBCC CHECKIDENT` |
| **Autocommit** | `conn.autocommit = True` | `conn.autocommit(True)` |

### 13.3 Data Flow (SQL Server)
```
1. Extract: Postgres COPY TO STDOUT (CSV format)
   ↓
2. Buffer: SpooledTemporaryFile (binary mode, 128MB cap)
   ↓
3. Transform: io.TextIOWrapper + Data Conversions
   - Strip datetime timezone (+00, Z)
   - Convert booleans (t/f → 1/0)
   - Handle empty strings vs NULL
   - Match columns to target schema
   ↓
4. Load: SQL Server batch INSERT (1000 rows/batch)
   - Disable FK constraints
   - Enable IDENTITY_INSERT (if needed)
   - Batch insert data
   - Re-enable FK constraints
```

### 13.4 Key Implementation Details

**Database Creation**:
- DAG connects to `master` database initially
- Creates `pagila_target` database if it doesn't exist
- Switches context using `USE pagila_target;` before operations

**Schema Reset**:
- Drops foreign key constraints first (SQL Server requirement)
- Drops all tables in `dbo` schema
- Recreates schema from `schema_mssql.sql`

**Column Matching**:
- Queries target schema to get actual columns (`INFORMATION_SCHEMA.COLUMNS`)
- Only copies matching columns from source (handles schema differences)
- Queries `IS_NULLABLE` to determine which columns accept NULL

**Data Transformations**:
- **Datetime**: Strip timezone info (`2022-01-15 10:00:00+00` → `2022-01-15 10:00:00`)
- **Boolean**: Convert Postgres format (`t`/`f` → `1`/`0` for BIT columns)
- **Empty Strings**: Keep as `""` for NOT NULL columns, convert to NULL for nullable columns
- **IDENTITY_INSERT**: Query `sys.identity_columns`, only enable for tables with identity

**Data Copy**:
- Uses `DELETE FROM` instead of `TRUNCATE` (foreign key compatibility)
- Disables FK constraints during load: `ALTER TABLE NOCHECK CONSTRAINT ALL`
- Re-enables FK constraints after load: `ALTER TABLE CHECK CONSTRAINT ALL`
- Batches INSERT statements (1000 rows per batch)
- Uses `%s` placeholders (pymssql convention)

**Identity Reseeding**:
- Uses `DBCC CHECKIDENT` to reset auto-increment values
- Ensures new rows continue from correct sequence

### 13.5 Data Type Conversions

The SQL Server schema (`schema_mssql.sql`) converts Postgres types:

| Postgres Type | SQL Server Type | Notes |
|--------------|----------------|-------|
| `SERIAL` | `INT IDENTITY(1,1)` | Auto-increment |
| `BIGSERIAL` | `BIGINT IDENTITY(1,1)` | Large auto-increment |
| `TEXT` | `NVARCHAR(MAX)` | Variable length |
| `VARCHAR(n)` | `NVARCHAR(n)` | Unicode support |
| `TIMESTAMP` | `DATETIME2` | Timezone stripped |
| `BOOLEAN` | `BIT` | Values converted |
| `BYTEA` | `VARBINARY(MAX)` | Binary data |
| `NUMERIC(p,s)` | `DECIMAL(p,s)` | Same semantics |

**Removed Features**:
- `fulltext` column (tsvector) - No direct SQL Server equivalent
- DOMAIN types - Converted to base types
- ENUM types - Converted to NVARCHAR with constraints
- Postgres functions/triggers - Not needed for replication

### 13.6 Switching Between Postgres and SQL Server

To switch from Postgres to SQL Server target:

1. **Start SQL Server container** (see Section 3.2, Option B)
2. **Connect to Astro network** (see Section 3.3)
3. **Create SQL Server connection** (see Section 4.2, Option B)
4. **Update requirements.txt**:
   ```
   apache-airflow-providers-microsoft-mssql
   pymssql
   ```
5. **Restart Astro**: `astro dev restart`
6. **Modify DAG**: In `replicate_pagila_to_target.py`, change:
   ```python
   TGT_CONN_ID = "pagila_mssql"  # Change from "pagila_tgt"
   ```
7. **Trigger replication**: `astro dev run dags trigger replicate_pagila_to_target`

To switch back to Postgres, reverse these changes.

### 13.7 Troubleshooting SQL Server

**Error: "Cannot insert explicit value for identity column when IDENTITY_INSERT is set to OFF"**
- Solution: DAG now detects identity columns and enables IDENTITY_INSERT automatically
- Verify you're running the latest version with this fix

**Error: "Conversion failed when converting date and/or time from character string"**
- Solution: DAG strips timezone info from Postgres timestamps (+00, Z)
- Verify you're running the latest version with datetime conversion

**Error: "Conversion failed when converting the nvarchar value 't' to data type bit"**
- Solution: DAG converts Postgres boolean values (t/f → 1/0)
- Verify you're running the latest version with boolean conversion

**Error: "Cannot insert the value NULL into column 'phone'; column does not allow nulls"**
- Solution: DAG queries IS_NULLABLE and handles empty strings correctly
- Empty strings stay as "" for NOT NULL columns, convert to NULL for nullable

**Error: "There are fewer columns in the INSERT statement than values"**
- Solution: DAG queries target schema and only copies matching columns
- Handles schema differences (e.g., fulltext column in Postgres but not SQL Server)

**Error: "INSERT statement conflicted with FOREIGN KEY constraint"**
- Solution: DAG disables FK constraints during load with NOCHECK CONSTRAINT ALL
- Circular dependencies (store ↔ staff) are handled automatically

**Error: "String or binary data would be truncated"**
- Solution: Check `schema_mssql.sql` column sizes match your data
- Example fix: `username NVARCHAR(16)` → `NVARCHAR(50)` for longer usernames

**Error: "Cannot truncate table because it is being referenced by a FOREIGN KEY constraint"**
- Solution: DAG uses `DELETE FROM` instead of `TRUNCATE`
- Verify you're running the latest version with this fix

**Error: "Incorrect syntax near 'GO'"**
- Solution: `GO` statements are removed from `schema_mssql.sql`
- If you manually edited the schema, remove all `GO` batch separators

**Error: "object attribute 'autocommit' is read-only"**
- Solution: DAG uses `conn.autocommit(True)` method call, not attribute assignment
- Verify you're using the latest DAG code

**Error: "Database 'pagila_target' does not exist"**
- Solution: The DAG creates this database automatically
- Ensure connection uses `master` schema (not `pagila_target`)

**Error: "column 'last_update' does not exist" on payment table**
- Solution: Postgres payment table is partitioned and doesn't have last_update
- Ensure `schema_mssql.sql` payment table doesn't include last_update column

**Performance: Large tables spilling to disk**
- Solution: Increase `SPOOLED_MAX_MEMORY_BYTES` in the DAG
- Default is 128MB; adjust based on worker memory capacity

**Performance: Slow replication**
- Solution: Increase batch size from 1000 to 5000 or 10000 rows
- Monitor memory usage to avoid OOM errors

For detailed migration notes and all 14 challenges solved, see `MSSQL_MIGRATION.md`.

---

## 14. Contributor Notes
- The contributor guide lives in `AGENTS.md` and expands on project structure, coding style, testing commands, and pull-request expectations.
- Run `astro dev pytest tests/dags` (requires Docker permissions) before opening a pull request; it covers DAG imports, tags, and retry policies.
- If you add new DAGs, mirror the shared constants pattern (`DEFAULT_ARGS`, connection IDs, table lists) so that operators remain easy to audit and tests pass without modification.
