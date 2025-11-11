from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

SRC_CONN_ID = "stackoverflow_source"
TGT_CONN_ID = "stackoverflow_postgres_target"

# Shared volume paths
# Airflow sees: /usr/local/airflow/include/bulk_files/
# PostgreSQL sees: /bulk_files/
AIRFLOW_BULK_DIR = Path("/usr/local/airflow/include/bulk_files")
POSTGRES_BULK_DIR = Path("/bulk_files")

# Partition size for large tables (rows per chunk)
PARTITION_SIZE = 500_000

# Table groupings for smart parallel execution
LOOKUP_TABLES = ["VoteTypes", "PostTypes", "LinkTypes"]
USER_DEPENDENT_TABLES = ["Badges", "Posts"]
POST_DEPENDENT_TABLES = ["Comments", "Votes", "PostLinks"]

# Tables that need partitioning (> 1M rows)
LARGE_TABLES = {
    "Votes": 10_000_000,      # Estimate: ~10M rows
    "Posts": 1_700_000,       # Estimate: ~1.7M rows
    "Comments": 1_300_000,    # Estimate: ~1.3M rows
}

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

log = logging.getLogger(__name__)


def reset_target_schema() -> None:
    """Create stackoverflow_target database and drop all tables on PostgreSQL target."""
    hook = PostgresHook(postgres_conn_id=TGT_CONN_ID)

    # First, connect to default database to create stackoverflow_target if needed
    conn = hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Check if database exists
    cur.execute("""
        SELECT 1 FROM pg_database WHERE datname = 'stackoverflow_target'
    """)

    if cur.fetchone() is None:
        # Create database
        cur.execute("CREATE DATABASE stackoverflow_target")
        log.info("Created stackoverflow_target database")
    else:
        log.info("stackoverflow_target database already exists")

    cur.close()
    conn.close()

    # Now connect to stackoverflow_target to drop tables
    hook_target = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')
    conn = hook_target.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Drop all foreign key constraints first
    cur.execute("""
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN (
                SELECT constraint_name, table_name
                FROM information_schema.table_constraints
                WHERE constraint_type = 'FOREIGN KEY'
                  AND table_schema = 'public'
            ) LOOP
                EXECUTE 'ALTER TABLE ' || quote_ident(r.table_name) || ' DROP CONSTRAINT ' || quote_ident(r.constraint_name);
            END LOOP;
        END $$;
    """)

    # Drop all tables in public schema
    cur.execute("""
        DO $$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN (
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
            ) LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
        END $$;
    """)
    log.info("Dropped all existing tables in public schema")

    cur.close()
    conn.close()


def map_sqlserver_to_postgres_type(data_type: str, char_len: int | None, num_prec: int | None, num_scale: int | None) -> str:
    """
    Map SQL Server data types to PostgreSQL data types.

    Args:
        data_type: SQL Server data type name
        char_len: Character length for string types (-1 for MAX)
        num_prec: Numeric precision
        num_scale: Numeric scale

    Returns:
        PostgreSQL data type string
    """
    data_type_lower = data_type.lower()

    # String types
    if data_type_lower in ('varchar', 'nvarchar', 'char', 'nchar'):
        if char_len == -1:
            return "TEXT"
        else:
            return f"VARCHAR({char_len})"

    # Numeric types
    elif data_type_lower in ('decimal', 'numeric'):
        return f"NUMERIC({num_prec},{num_scale or 0})"
    elif data_type_lower in ('int', 'integer'):
        return "INTEGER"
    elif data_type_lower == 'bigint':
        return "BIGINT"
    elif data_type_lower == 'smallint':
        return "SMALLINT"
    elif data_type_lower == 'tinyint':
        return "SMALLINT"  # PostgreSQL doesn't have TINYINT, use SMALLINT
    elif data_type_lower == 'bit':
        return "BOOLEAN"
    elif data_type_lower == 'float':
        return "DOUBLE PRECISION"
    elif data_type_lower == 'real':
        return "REAL"

    # Date/Time types
    elif data_type_lower in ('datetime', 'datetime2', 'smalldatetime'):
        return "TIMESTAMP"
    elif data_type_lower == 'date':
        return "DATE"
    elif data_type_lower == 'time':
        return "TIME"

    # Binary types
    elif data_type_lower in ('varbinary', 'binary'):
        return "BYTEA"

    # Text types
    elif data_type_lower in ('text', 'ntext'):
        return "TEXT"

    # XML
    elif data_type_lower == 'xml':
        return "XML"

    # UUID/uniqueidentifier
    elif data_type_lower == 'uniqueidentifier':
        return "UUID"

    # Default fallback
    else:
        log.warning(f"Unknown SQL Server type: {data_type}, using TEXT as fallback")
        return "TEXT"


def create_target_schema() -> None:
    """
    Dynamically create target schema by copying table structures from SQL Server source.
    Creates UNLOGGED tables without indexes/constraints for maximum bulk load performance.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    src_conn = src_hook.get_conn()
    src_conn.autocommit(True)
    src_cur = src_conn.cursor()

    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit = True
    tgt_cur = tgt_conn.cursor()

    # Get all tables from source
    all_tables = LOOKUP_TABLES + ["Users"] + USER_DEPENDENT_TABLES + POST_DEPENDENT_TABLES

    # For each table, get CREATE TABLE script from source
    for table in all_tables:
        log.info(f"Creating UNLOGGED table schema for {table} (no indexes/constraints)")

        # Get column definitions from source
        src_cur.execute("""
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_NAME = %s
            ORDER BY c.ORDINAL_POSITION
        """, (table,))

        columns = src_cur.fetchall()

        if not columns:
            log.warning(f"Table {table} not found in source database, skipping")
            continue

        # Build CREATE TABLE statement (NO PRIMARY KEYS OR INDEXES)
        col_defs = []
        for col in columns:
            col_name, data_type, char_len, num_prec, num_scale, is_nullable, is_identity = col

            # Map SQL Server type to PostgreSQL type
            pg_type = map_sqlserver_to_postgres_type(data_type, char_len, num_prec, num_scale)

            # Build column definition
            col_def = f'"{col_name}" {pg_type}'

            # Note: We do NOT add GENERATED AS IDENTITY here - we'll add it after bulk load
            # This allows us to insert explicit ID values during COPY

            # Allow NULL for text columns to handle empty string conversion
            if is_nullable == 'NO' and pg_type not in ('TEXT', 'VARCHAR') and not pg_type.startswith('VARCHAR('):
                col_def += " NOT NULL"
            else:
                col_def += " NULL"

            col_defs.append(col_def)

        # Create UNLOGGED table (no WAL overhead, no indexes)
        create_sql = f'CREATE UNLOGGED TABLE "{table}" (\n  {",\n  ".join(col_defs)}\n);'
        tgt_cur.execute(create_sql)
        log.info(f"Created UNLOGGED table {table} (no PK/indexes for bulk load)")

    src_cur.close()
    src_conn.close()
    tgt_cur.close()
    tgt_conn.close()

    log.info("Target schema created successfully (UNLOGGED tables, no constraints)")


def optimize_postgres_for_bulk() -> None:
    """Configure PostgreSQL for maximum bulk loading performance."""
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')
    conn = tgt_hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    log.info("Configuring PostgreSQL for aggressive bulk loading")

    # Increase memory for bulk operations (runtime changeable)
    cur.execute("SET maintenance_work_mem = '1GB'")
    cur.execute("SET work_mem = '512MB'")

    # Disable synchronous commit for speed (data in memory, not fsync'd to disk)
    cur.execute("SET synchronous_commit = OFF")

    # Note: wal_buffers and max_parallel_workers_per_gather require server restart, skipping

    log.info("PostgreSQL optimized: maintenance_work_mem=1GB, work_mem=512MB, synchronous_commit=OFF")

    cur.close()
    conn.close()


def export_table_to_csv(table: str, partition_id: int | None = None, offset: int = 0, limit: int | None = None) -> str:
    """
    Export table from SQL Server to CSV in shared volume.

    Args:
        table: Table name to export
        partition_id: Partition number (for chunked exports)
        offset: Row offset for partitioning
        limit: Number of rows to export (for partitioning)

    Returns:
        Path to CSV file (as seen by PostgreSQL container)
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)

    # Determine filename
    if partition_id is not None:
        csv_filename = f"{table}_part{partition_id:03d}.csv"
    else:
        csv_filename = f"{table}.csv"

    airflow_csv_path = AIRFLOW_BULK_DIR / csv_filename
    postgres_csv_path = POSTGRES_BULK_DIR / csv_filename

    # Ensure directory exists
    AIRFLOW_BULK_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"Exporting {table} to {airflow_csv_path} (offset={offset}, limit={limit})")

    # Get column names
    with src_hook.get_conn() as conn:
        conn.autocommit(True)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (table,))
            columns = [row[0] for row in cur.fetchall()]

            if not columns:
                raise RuntimeError(f"Table {table} has no columns")

            column_list = ", ".join(f"[{col}]" for col in columns)

            # Build query with optional OFFSET/FETCH for partitioning
            if limit is not None:
                query = f"""
                    SELECT {column_list}
                    FROM dbo.[{table}]
                    ORDER BY (SELECT NULL)
                    OFFSET {offset} ROWS
                    FETCH NEXT {limit} ROWS ONLY
                """
            else:
                query = f"SELECT {column_list} FROM dbo.[{table}]"

            cur.execute(query)

            # Write to CSV
            with open(airflow_csv_path, 'w', encoding='utf-8', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)

                row_count = 0
                for row in cur:
                    # Convert values to CSV-safe format
                    csv_row = []
                    for v in row:
                        if v is None:
                            csv_row.append('')
                        elif isinstance(v, datetime):
                            csv_row.append(v.strftime('%Y-%m-%d %H:%M:%S'))
                        elif isinstance(v, bool):
                            csv_row.append('t' if v else 'f')
                        else:
                            csv_row.append(str(v))
                    csv_writer.writerow(csv_row)
                    row_count += 1

    log.info(f"Exported {row_count} rows from {table} to {airflow_csv_path}")

    # Return path as PostgreSQL sees it
    return str(postgres_csv_path)


def get_table_row_count(table: str) -> int:
    """Get total row count for a table from SQL Server source."""
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    with src_hook.get_conn() as conn:
        conn.autocommit(True)
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM dbo.[{table}]")
            count = cur.fetchone()[0]
            log.info(f"Table {table} has {count:,} rows")
            return count


def export_table_partitioned(table: str) -> List[str]:
    """
    Export large table in partitions to multiple CSV files.

    Returns:
        List of CSV file paths (as seen by PostgreSQL)
    """
    row_count = get_table_row_count(table)
    csv_files = []

    if row_count <= PARTITION_SIZE:
        # Small table, export in one go
        csv_path = export_table_to_csv(table)
        csv_files.append(csv_path)
    else:
        # Large table, partition it
        num_partitions = (row_count + PARTITION_SIZE - 1) // PARTITION_SIZE
        log.info(f"Partitioning {table} into {num_partitions} chunks of {PARTITION_SIZE:,} rows")

        for i in range(num_partitions):
            offset = i * PARTITION_SIZE
            csv_path = export_table_to_csv(
                table,
                partition_id=i,
                offset=offset,
                limit=PARTITION_SIZE
            )
            csv_files.append(csv_path)

    return csv_files


def bulk_copy_to_postgres(table: str, csv_file: str) -> None:
    """
    Load CSV file into PostgreSQL using COPY command.

    Args:
        table: Target table name
        csv_file: Path to CSV file (as seen by PostgreSQL container)
    """
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    log.info(f"Loading {csv_file} into {table} using COPY")

    # Get connection without context manager to avoid auto-rollback on exit
    conn = tgt_hook.get_conn()
    try:
        # Use autocommit mode for immediate persistence (required for pg8000)
        conn.autocommit = True

        with conn.cursor() as cur:
            # Get column names
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table,))
            columns = [row[0] for row in cur.fetchall()]

            if not columns:
                raise RuntimeError(f"Target table {table} has no columns")

            column_list = ", ".join(f'"{col}"' for col in columns)

            # COPY from CSV (each COPY commits immediately in autocommit mode)
            copy_sql = f"COPY \"{table}\" ({column_list}) FROM '{csv_file}' WITH (FORMAT CSV, NULL '')"
            cur.execute(copy_sql)

            # Get row count
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            row_count = cur.fetchone()[0]

            log.info(f"Successfully loaded {csv_file} into {table} ({row_count:,} total rows)")
    except Exception as e:
        log.error(f"Failed to load {csv_file} into {table}: {e}")
        raise
    finally:
        conn.close()


def export_and_load_table(table: str) -> None:
    """
    Export table from SQL Server and load into PostgreSQL.
    Handles both single and partitioned exports.
    """
    log.info(f"Starting export and load for {table}")

    # Export to CSV (may create multiple files for large tables)
    csv_files = export_table_partitioned(table)

    # Load each CSV file
    for csv_file in csv_files:
        bulk_copy_to_postgres(table, csv_file)

    log.info(f"Completed export and load for {table} ({len(csv_files)} file(s))")


def add_indexes_and_constraints() -> None:
    """Add primary keys and indexes after bulk loading is complete."""
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    src_conn = src_hook.get_conn()
    src_conn.autocommit(True)
    src_cur = src_conn.cursor()

    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit = True
    tgt_cur = tgt_conn.cursor()

    all_tables = LOOKUP_TABLES + ["Users"] + USER_DEPENDENT_TABLES + POST_DEPENDENT_TABLES

    log.info("Adding primary keys and indexes after bulk load")

    for table in all_tables:
        # Get primary key from source
        src_cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = %s
              AND CONSTRAINT_NAME LIKE 'PK_%'
            ORDER BY ORDINAL_POSITION
        """, (table,))

        pk_cols = [row[0] for row in src_cur.fetchall()]

        if pk_cols:
            try:
                pk_def = ", ".join(f'"{col}"' for col in pk_cols)
                tgt_cur.execute(f'ALTER TABLE "{table}" ADD PRIMARY KEY ({pk_def})')
                log.info(f"Added primary key to {table} ({pk_def})")
            except Exception as e:
                log.warning(f"Failed to add primary key to {table}: {e}")

        # Add identity property to ID columns
        src_cur.execute("""
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_NAME = %s
              AND COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') = 1
        """, (table,))

        identity_cols = [row[0] for row in src_cur.fetchall()]

        for id_col in identity_cols:
            try:
                tgt_cur.execute(f'ALTER TABLE "{table}" ALTER COLUMN "{id_col}" ADD GENERATED ALWAYS AS IDENTITY')
                log.info(f"Added IDENTITY to {table}.{id_col}")
            except Exception as e:
                log.warning(f"Failed to add IDENTITY to {table}.{id_col}: {e}")

    src_cur.close()
    src_conn.close()
    tgt_cur.close()
    tgt_conn.close()

    log.info("Indexes and constraints added successfully")


def convert_tables_to_logged() -> None:
    """Convert all UNLOGGED tables to regular logged tables for durability."""
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')
    conn = tgt_hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    all_tables = LOOKUP_TABLES + ["Users"] + USER_DEPENDENT_TABLES + POST_DEPENDENT_TABLES

    log.info("Converting UNLOGGED tables to regular logged tables")

    for table in all_tables:
        try:
            cur.execute(f'ALTER TABLE "{table}" SET LOGGED')
            log.info(f"Converted {table} to LOGGED table")
        except Exception as e:
            log.warning(f"Failed to convert {table} to LOGGED: {e}")

    cur.close()
    conn.close()
    log.info("Table conversion completed")


def set_identity_sequences() -> None:
    """Reset identity sequences on PostgreSQL target for Stack Overflow tables."""
    tgt = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')
    conn = tgt.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Stack Overflow identity columns (table, primary_key_column)
    identity_tables = [
        ("Users", "Id"),
        ("Posts", "Id"),
        ("Comments", "Id"),
        ("Votes", "Id"),
        ("Badges", "Id"),
        ("PostLinks", "Id"),
        ("VoteTypes", "Id"),
        ("PostTypes", "Id"),
        ("LinkTypes", "Id"),
    ]

    for table, pk in identity_tables:
        # Check if table exists
        cur.execute(
            'SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s',
            ('public', table)
        )
        if cur.fetchone() is None:
            log.info(f"Table {table} not found, skipping sequence alignment")
            continue

        # Get max ID
        cur.execute(f'SELECT COALESCE(MAX("{pk}"), 0) FROM "{table}"')
        (max_id,) = cur.fetchone()

        if max_id > 0:
            # Get the sequence name for this identity column
            cur.execute("""
                SELECT pg_get_serial_sequence(%s, %s)
            """, (f'"{table}"', pk))
            seq_result = cur.fetchone()

            if seq_result and seq_result[0]:
                sequence_name = seq_result[0]
                # Reset sequence to max value + 1
                cur.execute(f"SELECT setval(%s, %s)", (sequence_name, max_id))
                log.info(f"Reset sequence for {table}.{pk} to {max_id}")
            else:
                log.warning(f"No sequence found for {table}.{pk}")
        else:
            log.info(f"Table {table} is empty, skipping sequence alignment")

    cur.close()
    conn.close()


with DAG(
    dag_id="replicate_stackoverflow_to_postgres_bulk_parallel",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stackoverflow", "postgres", "replication", "bulk", "parallel", "optimized"],
    template_searchpath=["/usr/local/airflow/include"],
    description="BULK PARALLEL: SQL Server to PostgreSQL with shared volume COPY and smart parallel groups",
) as dag:
    reset_tgt = PythonOperator(
        task_id="reset_target_schema",
        python_callable=reset_target_schema,
    )

    create_schema = PythonOperator(
        task_id="create_target_schema",
        python_callable=create_target_schema,
    )

    optimize_pg = PythonOperator(
        task_id="optimize_postgres_for_bulk",
        python_callable=optimize_postgres_for_bulk,
    )

    # ALL TABLES IN PARALLEL - No FK constraints during load phase
    # Create list to hold all load tasks
    all_load_tasks = []

    # All tables from all groups
    all_tables = LOOKUP_TABLES + ["Users"] + USER_DEPENDENT_TABLES + POST_DEPENDENT_TABLES

    # Create parallel load tasks for ALL tables
    for table in all_tables:
        task = PythonOperator(
            task_id=f"export_load_{table}",
            python_callable=export_and_load_table,
            op_kwargs={"table": table},
        )
        all_load_tasks.append(task)

    add_indexes = PythonOperator(
        task_id="add_indexes_and_constraints",
        python_callable=add_indexes_and_constraints,
    )

    convert_to_logged = PythonOperator(
        task_id="convert_tables_to_logged",
        python_callable=convert_tables_to_logged,
    )

    fix_sequences = PythonOperator(
        task_id="align_target_sequences",
        python_callable=set_identity_sequences,
    )

    # MAXIMUM PARALLEL DEPENDENCY GRAPH
    # All 9 tables load in parallel (no FK constraints during load phase)
    # Reset → Schema → Optimize → ALL TABLES (parallel) → Indexes → Convert → Sequences
    reset_tgt >> create_schema >> optimize_pg >> all_load_tasks >> add_indexes >> convert_to_logged >> fix_sequences
