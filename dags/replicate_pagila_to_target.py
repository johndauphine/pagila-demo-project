from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timedelta
from tempfile import SpooledTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

SRC_CONN_ID = "pagila_postgres"
TGT_CONN_ID = "pagila_mssql"

# Target is now SQL Server, so skip Postgres role creation
ENSURE_POSTGRES_ROLE_ON_TARGET = False

# Disk-backed streaming configuration (SpooledTemporaryFile)
SPOOLED_MAX_MEMORY_BYTES = 128 * 1024 * 1024  # spill to disk above ~128 MB

log = logging.getLogger(__name__)

TABLE_ORDER = [
    "language",
    "category",
    "actor",
    "film",
    "film_actor",
    "film_category",
    "country",
    "city",
    "address",
    "staff",      # Must come before store due to circular FK
    "store",
    "customer",
    "inventory",
    "rental",
    "payment",
]

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def ensure_tgt_roles() -> None:
    """No-op for SQL Server target; kept for DAG compatibility."""
    log.info("Skipping role creation (SQL Server target)")


def reset_target_schema() -> None:
    """Create pagila_target database and drop all tables in dbo schema on SQL Server target."""
    hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()
    
    # Create database if it doesn't exist (connected to master)
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'pagila_target')
        BEGIN
            CREATE DATABASE pagila_target;
        END
    """)
    log.info("Ensured pagila_target database exists")
    
    # Switch to pagila_target database
    cur.execute("USE pagila_target;")
    
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
    """Load the SQL Server schema from schema_mssql.sql file."""
    schema_path = "/usr/local/airflow/include/pagila/schema_mssql.sql"
    
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    
    hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()
    
    # Execute the schema file (it includes USE pagila_target)
    cur.execute(schema_sql)
    log.info("Created target schema from schema_mssql.sql")
    
    cur.close()
    conn.close()


def copy_table_src_to_tgt(table: str) -> None:
    """
    Copy table from Postgres source to SQL Server target.
    
    Strategy: Read from Postgres via COPY TO STDOUT (CSV format),
    write to SQL Server via bulk insert using CSV data.
    """
    src_hook = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    log.info(
        "[%s] starting buffered copy (memory cap≈%.1f MB)",
        table,
        SPOOLED_MAX_MEMORY_BYTES / (1024 * 1024),
    )

    # Get target table columns first to ensure we only copy matching columns
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    with tgt_hook.get_conn() as tgt_conn_tmp:
        tgt_conn_tmp.autocommit(True)
        with tgt_conn_tmp.cursor() as tgt_cur_tmp:
            tgt_cur_tmp.execute("USE pagila_target;")
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
    
    column_list_for_select = ", ".join(f'"{col}"' for col in target_columns)

    # Use binary mode for SpooledTemporaryFile (COPY outputs bytes)
    with SpooledTemporaryFile(
        max_size=SPOOLED_MAX_MEMORY_BYTES, mode="w+b"
    ) as spool:
        # Read from Postgres using COPY TO CSV - only select columns that exist in target
        with src_hook.get_conn() as src_conn:
            src_conn.autocommit = True
            with src_conn.cursor() as src_cur:
                # Export as CSV with proper escaping, only selecting target columns
                copy_sql = f"""
                    COPY (SELECT {column_list_for_select} FROM public.{table}) 
                    TO STDOUT WITH (FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '"', ESCAPE '"')
                """
                src_cur.copy_expert(copy_sql, spool)

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
        
        # Wrap binary spool in text wrapper for CSV reading
        text_spool = io.TextIOWrapper(spool, encoding='utf-8', newline='')

        # Write to SQL Server using bulk insert
        tgt_conn = tgt_hook.get_conn()
        tgt_conn.autocommit(False)
        tgt_cur = tgt_conn.cursor()
        
        try:
            # Switch to pagila_target database
            tgt_cur.execute("USE pagila_target;")
            
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

            # Delete all rows from target table (use DELETE instead of TRUNCATE due to foreign keys)
            tgt_cur.execute(f"DELETE FROM dbo.[{table}]")

            # Disable FK checks for this table to handle circular dependencies
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

            # Enable IDENTITY_INSERT only if table has an identity column
            if has_identity:
                tgt_cur.execute(f"SET IDENTITY_INSERT dbo.[{table}] ON")

            # Read CSV and insert rows
            row_count = 0
            csv_reader = csv.reader(text_spool)
            batch_size = 1000
            batch = []

            for row in csv_reader:
                # Convert values: handle timestamps, booleans, and empty strings
                processed_row = []
                for idx, v in enumerate(row):
                    col_name = target_columns[idx]
                    
                    if v == "":
                        # Only convert empty strings to NULL if column allows NULL
                        # Otherwise keep as empty string for NOT NULL columns
                        if col_name in nullable_columns:
                            processed_row.append(None)
                        else:
                            processed_row.append("")
                    elif v == "t":  # Postgres boolean true
                        processed_row.append("1")
                    elif v == "f":  # Postgres boolean false
                        processed_row.append("0")
                    elif "+" in v and " " in v:  # Looks like a timestamp with timezone
                        # Remove timezone info for SQL Server DATETIME2
                        # Postgres format: "2022-02-15 10:02:19+00"
                        clean_ts = v.split("+")[0]
                        processed_row.append(clean_ts)
                    elif v.endswith("Z") and "T" in v:  # ISO 8601 with Z
                        # Convert "2022-02-15T10:02:19Z" to "2022-02-15 10:02:19"
                        clean_ts = v.replace("T", " ").replace("Z", "")
                        processed_row.append(clean_ts)
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
                    batch = []

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

            # Disable IDENTITY_INSERT only if we enabled it
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
    """Reseed identity columns on SQL Server target."""
    tgt = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = tgt.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()
    
    # Switch to pagila_target database
    cur.execute("USE pagila_target;")
    
    identity_tables = [
        ("actor", "actor_id"),
        ("category", "category_id"),
        ("film", "film_id"),
        ("customer", "customer_id"),
        ("staff", "staff_id"),
        ("store", "store_id"),
        ("inventory", "inventory_id"),
        ("rental", "rental_id"),
        ("payment", "payment_id"),
        ("city", "city_id"),
        ("address", "address_id"),
        ("country", "country_id"),
        ("language", "language_id"),
    ]
    for table, pk in identity_tables:
        cur.execute(f"SELECT COALESCE(MAX([{pk}]), 0) FROM dbo.[{table}]")
        (max_id,) = cur.fetchone()
        if max_id > 0:
            # Reseed identity to max value
            cur.execute(f"DBCC CHECKIDENT ('dbo.{table}', RESEED, {max_id})")
            log.info(f"Reseeded {table}.{pk} to {max_id}")
    
    cur.close()
    conn.close()


with DAG(
    dag_id="replicate_pagila_to_target",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pagila", "postgres", "mssql", "replication"],
    template_searchpath=["/usr/local/airflow/include"],
) as dag:
    reset_tgt = PythonOperator(
        task_id="reset_target_schema",
        python_callable=reset_target_schema,
    )

    ensure_tgt_role = PythonOperator(
        task_id="ensure_postgres_role_on_target",
        python_callable=ensure_tgt_roles,
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

    if ENSURE_POSTGRES_ROLE_ON_TARGET:
        reset_tgt >> ensure_tgt_role >> create_schema
    else:
        reset_tgt >> create_schema

    prev >> fix_sequences
