from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timedelta
from tempfile import SpooledTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

SRC_CONN_ID = "stackoverflow_source"
TGT_CONN_ID = "stackoverflow_target"

# Disk-backed streaming configuration (SpooledTemporaryFile)
SPOOLED_MAX_MEMORY_BYTES = 128 * 1024 * 1024  # spill to disk above ~128 MB

log = logging.getLogger(__name__)

# Table replication order based on dependencies
# VoteTypes, PostTypes, LinkTypes are lookup tables
# Users is parent to many tables, Posts depends on Users, etc.
TABLE_ORDER = [
    "VoteTypes",      # Lookup table, no dependencies
    "PostTypes",      # Lookup table, no dependencies
    "LinkTypes",      # Lookup table, no dependencies
    "Users",          # Parent table for Posts, Comments, Badges
    "Badges",         # Depends on Users
    "Posts",          # Depends on Users and PostTypes
    "PostLinks",      # Depends on Posts and LinkTypes
    "Comments",       # Depends on Posts and Users
    "Votes",          # Depends on Posts, Users, and VoteTypes
]

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def reset_target_schema() -> None:
    """Create stackoverflow_target database and drop all tables in dbo schema on SQL Server target."""
    hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()

    # Create database if it doesn't exist (connected to master)
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'stackoverflow_target')
        BEGIN
            CREATE DATABASE stackoverflow_target;
        END
    """)
    log.info("Ensured stackoverflow_target database exists")

    # Switch to stackoverflow_target database
    cur.execute("USE stackoverflow_target;")

    # Drop all foreign key constraints first
    cur.execute("""
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql += 'ALTER TABLE [' + OBJECT_SCHEMA_NAME(parent_object_id) + '].[' + OBJECT_NAME(parent_object_id) + '] DROP CONSTRAINT [' + name + '];'
        FROM sys.foreign_keys;
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
    """)

    # Drop all tables in dbo schema
    cur.execute("""
        DECLARE @sql NVARCHAR(MAX) = N'';
        SELECT @sql += 'DROP TABLE [dbo].[' + name + '];'
        FROM sys.tables
        WHERE schema_id = SCHEMA_ID('dbo');
        IF LEN(@sql) > 0 EXEC sp_executesql @sql;
    """)
    log.info("Dropped all existing tables in dbo schema")

    cur.close()
    conn.close()


def create_target_schema() -> None:
    """
    Dynamically create target schema by copying table structures from source database.
    This replicates the Stack Overflow schema without foreign keys.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    src_conn = src_hook.get_conn()
    src_conn.autocommit(True)
    src_cur = src_conn.cursor()

    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit(True)
    tgt_cur = tgt_conn.cursor()

    # Switch to target database
    tgt_cur.execute("USE stackoverflow_target;")

    # For each table, get CREATE TABLE script from source
    for table in TABLE_ORDER:
        log.info(f"Creating table schema for {table}")

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

        # Build CREATE TABLE statement
        col_defs = []
        for col in columns:
            col_name, data_type, char_len, num_prec, num_scale, is_nullable, is_identity = col

            # Build column definition
            col_def = f"[{col_name}] {data_type.upper()}"

            # Add length/precision
            if char_len and data_type.lower() in ('varchar', 'nvarchar', 'char', 'nchar'):
                if char_len == -1:
                    col_def += "(MAX)"
                else:
                    col_def += f"({char_len})"
            elif num_prec and data_type.lower() in ('decimal', 'numeric'):
                col_def += f"({num_prec},{num_scale or 0})"

            # Add IDENTITY if source column is identity
            if is_identity:
                col_def += " IDENTITY(1,1)"

            # Add NULL/NOT NULL
            if is_nullable == 'NO':
                col_def += " NOT NULL"
            else:
                col_def += " NULL"

            col_defs.append(col_def)

        # Get primary key
        src_cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = %s
              AND CONSTRAINT_NAME LIKE 'PK_%'
            ORDER BY ORDINAL_POSITION
        """, (table,))

        pk_cols = [row[0] for row in src_cur.fetchall()]

        if pk_cols:
            pk_def = f"PRIMARY KEY ({', '.join(f'[{col}]' for col in pk_cols)})"
            col_defs.append(pk_def)

        # Create table on target
        create_sql = f"CREATE TABLE dbo.[{table}] (\n  {',\n  '.join(col_defs)}\n);"
        tgt_cur.execute(create_sql)
        log.info(f"Created table dbo.{table}")

    src_cur.close()
    src_conn.close()
    tgt_cur.close()
    tgt_conn.close()

    log.info("Target schema created successfully")


def copy_table_src_to_tgt(table: str) -> None:
    """
    Copy table from SQL Server source to SQL Server target.

    Strategy: Read from source via SELECT, write to target via CSV bulk insert.
    Uses memory-capped streaming with SpooledTemporaryFile.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    log.info(
        "[%s] starting buffered copy (memory cap≈%.1f MB)",
        table,
        SPOOLED_MAX_MEMORY_BYTES / (1024 * 1024),
    )

    # Get target table columns
    with tgt_hook.get_conn() as tgt_conn_tmp:
        tgt_conn_tmp.autocommit(True)
        with tgt_conn_tmp.cursor() as tgt_cur_tmp:
            tgt_cur_tmp.execute("USE stackoverflow_target;")
            tgt_cur_tmp.execute(
                """
                SELECT COLUMN_NAME, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
                """,
                (table,),
            )
            column_info = tgt_cur_tmp.fetchall()
            target_columns = [row[0] for row in column_info]
            nullable_columns = {row[0] for row in column_info if row[1] == 'YES'}

    if not target_columns:
        raise RuntimeError(f"Target table dbo.{table} has no columns")

    column_list_for_select = ", ".join(f"[{col}]" for col in target_columns)

    # Use text mode for CSV writing
    with SpooledTemporaryFile(
        max_size=SPOOLED_MAX_MEMORY_BYTES, mode="w+", encoding='utf-8', newline=''
    ) as spool:
        csv_writer = csv.writer(spool)

        # Read from source SQL Server
        with src_hook.get_conn() as src_conn:
            src_conn.autocommit(True)
            with src_conn.cursor() as src_cur:
                # Select all rows from source table
                src_cur.execute(f"SELECT {column_list_for_select} FROM dbo.[{table}]")

                # Write rows to CSV in memory/disk
                row_count_src = 0
                for row in src_cur:
                    # Convert values to CSV-safe format
                    csv_row = []
                    for v in row:
                        if v is None:
                            csv_row.append('')
                        elif isinstance(v, (datetime, type(None).__class__)):
                            # Format datetime as ISO string without microseconds
                            csv_row.append(v.strftime('%Y-%m-%d %H:%M:%S') if v else '')
                        else:
                            csv_row.append(str(v))
                    csv_writer.writerow(csv_row)
                    row_count_src += 1

        spool.flush()
        written_bytes = spool.tell()
        spilled_to_disk = bool(getattr(spool, "_rolled", False))
        log.info(
            "[%s] buffered %s bytes, %s rows from source (rolled_to_disk=%s)",
            table,
            written_bytes,
            row_count_src,
            spilled_to_disk,
        )

        spool.seek(0)

        # Write to SQL Server target using bulk insert
        tgt_conn = tgt_hook.get_conn()
        tgt_conn.autocommit(False)
        tgt_cur = tgt_conn.cursor()

        try:
            # Switch to stackoverflow_target database
            tgt_cur.execute("USE stackoverflow_target;")

            # Verify target table exists
            tgt_cur.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s
                """,
                (table,),
            )
            if tgt_cur.fetchone() is None:
                raise RuntimeError(
                    f"Target table dbo.{table} does not exist; did schema creation run?"
                )

            # Get column list
            tgt_cur.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
                """,
                (table,),
            )
            columns = [row[0] for row in tgt_cur.fetchall()]
            column_list = ", ".join(f"[{col}]" for col in columns)

            # Delete all rows from target table
            tgt_cur.execute(f"DELETE FROM dbo.[{table}]")

            # Disable FK checks for this table
            tgt_cur.execute(f"ALTER TABLE dbo.[{table}] NOCHECK CONSTRAINT ALL")

            # Check if table has an identity column
            tgt_cur.execute("""
                SELECT COUNT(*)
                FROM sys.identity_columns ic
                INNER JOIN sys.tables t ON ic.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = 'dbo' AND t.name = %s
            """, (table,))
            has_identity = tgt_cur.fetchone()[0] > 0

            # Enable IDENTITY_INSERT if needed
            if has_identity:
                tgt_cur.execute(f"SET IDENTITY_INSERT dbo.[{table}] ON")

            # Read CSV and insert rows with periodic commits
            row_count = 0
            csv_reader = csv.reader(spool)
            batch_size = 1000
            commit_frequency = 10000  # Commit every 10K rows to avoid overwhelming SQL Server
            batch = []
            rows_since_last_commit = 0

            for row in csv_reader:
                # Convert values: handle empty strings and NULL
                processed_row = []
                for idx, v in enumerate(row):
                    col_name = target_columns[idx]

                    if v == "":
                        # Convert empty string to NULL if column allows NULL
                        if col_name in nullable_columns:
                            processed_row.append(None)
                        else:
                            processed_row.append("")
                    else:
                        processed_row.append(v)

                batch.append(processed_row)

                if len(batch) >= batch_size:
                    placeholders = ", ".join(
                        f"({', '.join(['%s' for _ in processed_row])})"
                        for processed_row in batch
                    )
                    insert_sql = f"INSERT INTO dbo.[{table}] ({column_list}) VALUES {placeholders}"
                    flat_values = [val for row in batch for val in row]
                    tgt_cur.execute(insert_sql, flat_values)
                    row_count += len(batch)
                    rows_since_last_commit += len(batch)
                    batch = []

                    # Commit periodically to avoid large transactions
                    if rows_since_last_commit >= commit_frequency:
                        tgt_conn.commit()
                        log.info(f"[{table}] committed {row_count} rows")
                        rows_since_last_commit = 0

            # Insert remaining rows
            if batch:
                placeholders = ", ".join(
                    f"({', '.join(['%s' for _ in processed_row])})"
                    for processed_row in batch
                )
                insert_sql = f"INSERT INTO dbo.[{table}] ({column_list}) VALUES {placeholders}"
                flat_values = [val for row in batch for val in row]
                tgt_cur.execute(insert_sql, flat_values)
                row_count += len(batch)

            # Disable IDENTITY_INSERT if we enabled it
            if has_identity:
                tgt_cur.execute(f"SET IDENTITY_INSERT dbo.[{table}] OFF")

            # Re-enable FK checks
            tgt_cur.execute(f"ALTER TABLE dbo.[{table}] CHECK CONSTRAINT ALL")

            tgt_conn.commit()
            log.info("[%s] copy completed (bytes=%s, rows=%s)", table, written_bytes, row_count)
        finally:
            tgt_cur.close()
            tgt_conn.close()


def set_identity_sequences() -> None:
    """Reseed identity columns on SQL Server target for Stack Overflow tables."""
    tgt = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = tgt.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()

    # Switch to stackoverflow_target database
    cur.execute("USE stackoverflow_target;")

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
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s",
            (table,)
        )
        if cur.fetchone() is None:
            log.info(f"Table {table} not found, skipping sequence alignment")
            continue

        # Get max ID
        cur.execute(f"SELECT COALESCE(MAX([{pk}]), 0) FROM dbo.[{table}]")
        (max_id,) = cur.fetchone()

        if max_id > 0:
            # Reseed identity to max value
            cur.execute(f"DBCC CHECKIDENT ('dbo.{table}', RESEED, {max_id})")
            log.info(f"Reseeded {table}.{pk} to {max_id}")
        else:
            log.info(f"Table {table} is empty, skipping sequence alignment")

    cur.close()
    conn.close()


with DAG(
    dag_id="replicate_stackoverflow_to_target",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stackoverflow", "mssql", "replication", "sql-server"],
    template_searchpath=["/usr/local/airflow/include"],
) as dag:
    reset_tgt = PythonOperator(
        task_id="reset_target_schema",
        python_callable=reset_target_schema,
    )

    create_schema = PythonOperator(
        task_id="create_target_schema",
        python_callable=create_target_schema,
    )

    prev = create_schema
    for tbl in TABLE_ORDER:
        t = PythonOperator(
            task_id=f"copy_{tbl}",
            python_callable=copy_table_src_to_tgt,
            op_kwargs={"table": tbl},
        )
        prev >> t
        prev = t

    fix_sequences = PythonOperator(
        task_id="align_target_sequences",
        python_callable=set_identity_sequences,
    )

    reset_tgt >> create_schema
    prev >> fix_sequences
