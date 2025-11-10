from __future__ import annotations

import csv
import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

SRC_CONN_ID = "stackoverflow_source"
TGT_CONN_ID = "stackoverflow_target"

log = logging.getLogger(__name__)

# ALL tables - no dependencies since no foreign keys!
ALL_TABLES = [
    "VoteTypes",      # Small lookup table
    "PostTypes",      # Small lookup table
    "LinkTypes",      # Small lookup table
    "Users",          # 299K rows
    "Badges",         # 1.1M rows
    "Posts",          # 3.7M rows (largest)
    "PostLinks",      # ~100K rows
    "Comments",       # ~1.3M rows
    "Votes",          # ~10M rows (might be largest)
]

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Persistent directory for CSV files - bind mount shared between Airflow and SQL Server
BULK_DIR = "/usr/local/airflow/include/bulk_files"  # Path in Airflow container
SQL_BULK_DIR = "/bulk_files"  # Path in SQL Server container (same physical directory)


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

    # Set to BULK_LOGGED for optimal bulk load performance
    cur.execute("""
        ALTER DATABASE stackoverflow_target SET RECOVERY BULK_LOGGED;
    """)
    log.info("Set database to BULK_LOGGED recovery model")

    # Drop all foreign key constraints first (though there shouldn't be any)
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

    # Create bulk directory
    os.makedirs(BULK_DIR, exist_ok=True)
    log.info(f"Created bulk directory: {BULK_DIR}")

    cur.close()
    conn.close()


def create_target_schema() -> None:
    """
    Create HEAP TABLES (no indexes) optimized for BULK INSERT.
    Indexes will be created AFTER all data is loaded.
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

    # Store primary key info for later index creation
    pk_definitions = {}

    # For each table, get CREATE TABLE script from source
    for table in ALL_TABLES:
        log.info(f"Creating HEAP table schema for {table} (optimized for BULK INSERT)")

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

        # Build CREATE TABLE statement - NO CONSTRAINTS for bulk load speed!
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
                # IDENTITY columns must be NOT NULL
                col_def += " NOT NULL"
            else:
                # For BULK INSERT, allow NULLs everywhere initially
                col_def += " NULL"

            col_defs.append(col_def)

        # Get primary key info for later
        src_cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = %s
              AND CONSTRAINT_NAME LIKE 'PK_%'
            ORDER BY ORDINAL_POSITION
        """, (table,))

        pk_cols = [row[0] for row in src_cur.fetchall()]

        if pk_cols:
            pk_definitions[table] = pk_cols
            log.info(f"Will create PRIMARY KEY on {table}({', '.join(pk_cols)}) AFTER bulk load")

        # Create table as HEAP (no primary key yet - for maximum load speed!)
        create_sql = f"CREATE TABLE dbo.[{table}] (\n  {',\n  '.join(col_defs)}\n);"
        tgt_cur.execute(create_sql)
        log.info(f"Created HEAP table dbo.{table} (no indexes for bulk load)")

    # Store PK definitions in temp table for later use
    tgt_cur.execute("""
        IF OBJECT_ID('tempdb..##pk_definitions_bulk') IS NOT NULL DROP TABLE ##pk_definitions_bulk;
        CREATE TABLE ##pk_definitions_bulk (
            table_name NVARCHAR(128),
            pk_columns NVARCHAR(MAX)
        );
    """)

    for table, pk_cols in pk_definitions.items():
        tgt_cur.execute(
            "INSERT INTO ##pk_definitions_bulk VALUES (%s, %s)",
            (table, ','.join(pk_cols))
        )

    src_cur.close()
    src_conn.close()
    tgt_cur.close()
    tgt_conn.close()

    log.info("Target schema created - HEAP TABLES optimized for BULK INSERT!")


def export_and_bulk_insert_table(table: str) -> None:
    """
    Export to CSV in bind mount directory and BULK INSERT from shared volume.
    Uses bind mount that's accessible from both Airflow and SQL Server containers.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = MsSqlHook(mssql_conn_id=TGT_CONN_ID)

    # Use bind mount directory - accessible from both containers
    local_csv_file = f"{BULK_DIR}/{table}.csv"  # Path in Airflow container
    sql_csv_file = f"{SQL_BULK_DIR}/{table}.csv"  # Path in SQL Server container

    log.info(f"[{table}] Starting export to bind mount + BULK INSERT")

    # STEP 1: Export to CSV
    log.info(f"[{table}] Step 1: Exporting to CSV...")
    with src_hook.get_conn() as src_conn:
        src_conn.autocommit(True)
        with src_conn.cursor() as src_cur:
            # Get all data from source table
            src_cur.execute(f"SELECT * FROM dbo.[{table}]")

            # Write to CSV with tab delimiter
            with open(local_csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

                row_count = 0
                for row in src_cur:
                    # Handle NULL values - replace with empty string for CSV
                    processed_row = ['' if val is None else str(val) for val in row]
                    writer.writerow(processed_row)
                    row_count += 1

                    if row_count % 100000 == 0:
                        log.info(f"[{table}] Exported {row_count} rows...")

                # Force flush to disk
                f.flush()
                os.fsync(f.fileno())

            file_size = os.path.getsize(local_csv_file) / (1024 * 1024)  # MB
            log.info(f"[{table}] CSV export completed: {row_count} rows, {file_size:.2f} MB")

    # STEP 2: BULK INSERT from bind mount (no copy needed - direct access!)
    log.info(f"[{table}] Step 2: Executing BULK INSERT from bind mount...")
    with tgt_hook.get_conn() as tgt_conn:
        tgt_conn.autocommit(False)
        with tgt_conn.cursor() as tgt_cur:
            tgt_cur.execute("USE stackoverflow_target;")

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

            # Execute BULK INSERT
            bulk_sql = f"""
            BULK INSERT dbo.[{table}]
            FROM '{sql_csv_file}'
            WITH (
                FIELDTERMINATOR = '\t',
                ROWTERMINATOR = '\n',
                TABLOCK,
                BATCHSIZE = 10000,
                KEEPIDENTITY,
                MAXERRORS = 0
            )
            """

            tgt_cur.execute(bulk_sql)

            # Disable IDENTITY_INSERT if we enabled it
            if has_identity:
                tgt_cur.execute(f"SET IDENTITY_INSERT dbo.[{table}] OFF")

            tgt_conn.commit()

            # Get row count
            tgt_cur.execute(f"SELECT COUNT(*) FROM dbo.[{table}]")
            loaded_rows = tgt_cur.fetchone()[0]
            log.info(f"[{table}] BULK INSERT completed: {loaded_rows} rows loaded")

    # STEP 3: Cleanup
    log.info(f"[{table}] Step 3: Cleaning up CSV file from bind mount...")
    os.remove(local_csv_file)

    log.info(f"[{table}] Export + BULK INSERT completed successfully!")


def create_all_indexes() -> None:
    """
    Create ALL indexes and primary keys AFTER bulk load.
    This is MUCH faster than maintaining indexes during load!
    """
    tgt = MsSqlHook(mssql_conn_id=TGT_CONN_ID)
    conn = tgt.get_conn()
    conn.autocommit(True)
    cur = conn.cursor()

    log.info("Creating indexes and primary keys AFTER bulk load...")

    cur.execute("USE stackoverflow_target;")

    # Get PK definitions from temp table
    cur.execute("""
        IF OBJECT_ID('tempdb..##pk_definitions_bulk') IS NOT NULL
            SELECT table_name, pk_columns FROM ##pk_definitions_bulk
        ELSE
            SELECT NULL, NULL WHERE 1=0
    """)

    pk_defs = cur.fetchall()

    # Fallback to standard PKs if temp table is empty
    if not pk_defs:
        pk_defs = [
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
    else:
        pk_defs = [(table, cols.split(',')) for table, cols in pk_defs]
        pk_defs = [(table, cols[0] if len(cols) == 1 else cols) for table, cols in pk_defs]

    # Create primary keys
    for table, pk_col in pk_defs:
        # Check if table exists
        cur.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=%s",
            (table,)
        )
        if cur.fetchone()[0] == 0:
            log.info(f"Table {table} not found, skipping index creation")
            continue

        try:
            # Ensure PK columns are NOT NULL and have no NULL values
            if isinstance(pk_col, str):
                cur.execute(f"UPDATE dbo.[{table}] SET [{pk_col}] = 0 WHERE [{pk_col}] IS NULL")
                cur.execute(f"ALTER TABLE dbo.[{table}] ALTER COLUMN [{pk_col}] INT NOT NULL")
                pk_constraint = f"ALTER TABLE dbo.[{table}] ADD CONSTRAINT PK_{table} PRIMARY KEY CLUSTERED ([{pk_col}]) WITH (SORT_IN_TEMPDB = ON, ONLINE = OFF)"
            else:
                for col in pk_col:
                    cur.execute(f"UPDATE dbo.[{table}] SET [{col}] = 0 WHERE [{col}] IS NULL")
                    cur.execute(f"ALTER TABLE dbo.[{table}] ALTER COLUMN [{col}] INT NOT NULL")
                pk_constraint = f"ALTER TABLE dbo.[{table}] ADD CONSTRAINT PK_{table} PRIMARY KEY CLUSTERED ({', '.join(f'[{col}]' for col in pk_col)}) WITH (SORT_IN_TEMPDB = ON, ONLINE = OFF)"

            cur.execute(pk_constraint)
            log.info(f"Created PRIMARY KEY on {table}")

            # Update statistics after index creation
            cur.execute(f"UPDATE STATISTICS dbo.[{table}] WITH FULLSCAN")
            log.info(f"Updated statistics for {table}")

        except Exception as e:
            log.warning(f"Could not create PK on {table}: {e}")

    # Set recovery model back to FULL
    cur.execute("""
        ALTER DATABASE stackoverflow_target SET RECOVERY FULL;
    """)
    log.info("Set database back to FULL recovery model")

    cur.close()
    conn.close()

    log.info("All indexes and primary keys created successfully after bulk load!")


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


def cleanup_csv_files() -> None:
    """Clean up CSV files to save disk space."""
    import shutil
    if os.path.exists(BULK_DIR):
        shutil.rmtree(BULK_DIR)
        log.info(f"Cleaned up CSV files in {BULK_DIR}")


with DAG(
    dag_id="replicate_stackoverflow_to_target_bulk",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stackoverflow", "mssql", "replication", "sql-server", "bulk-insert", "optimized"],
    template_searchpath=["/usr/local/airflow/include"],
    description="BULK INSERT: Export to CSV, bulk load with T-SQL BULK INSERT, then create indexes",
) as dag:
    reset_tgt = PythonOperator(
        task_id="reset_target_schema",
        python_callable=reset_target_schema,
    )

    create_schema = PythonOperator(
        task_id="create_heap_tables",
        python_callable=create_target_schema,
    )

    # COMBINED: Export + BULK INSERT (all tables in parallel)
    combined_tasks = [
        PythonOperator(
            task_id=f"export_and_bulk_insert_{tbl}",
            python_callable=export_and_bulk_insert_table,
            op_kwargs={"table": tbl},
        )
        for tbl in ALL_TABLES
    ]

    # Create indexes AFTER all bulk loads
    create_indexes = PythonOperator(
        task_id="create_indexes_and_pks",
        python_callable=create_all_indexes,
    )

    fix_sequences = PythonOperator(
        task_id="align_target_sequences",
        python_callable=set_identity_sequences,
    )

    # Clean up CSV files
    cleanup = PythonOperator(
        task_id="cleanup_csv_files",
        python_callable=cleanup_csv_files,
        trigger_rule="all_done",  # Run even if something fails
    )

    # BULK INSERT PIPELINE:
    # Reset → Create schema → Export+BULK INSERT all (parallel) → Create indexes → Fix sequences → Cleanup
    reset_tgt >> create_schema

    # All combined tasks (export+import) run in parallel
    create_schema >> combined_tasks >> create_indexes

    # Create indexes → fix sequences → cleanup
    create_indexes >> fix_sequences >> cleanup
