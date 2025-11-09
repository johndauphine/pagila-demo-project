from __future__ import annotations

import csv
import logging
from datetime import datetime, timedelta
from tempfile import SpooledTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

SRC_CONN_ID = "stackoverflow_source"
TGT_CONN_ID = "stackoverflow_postgres_target"

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
    # Update connection to use stackoverflow_target database
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
        if char_len == -1:
            return "BYTEA"
        else:
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
    Converts SQL Server data types to PostgreSQL equivalents.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    src_conn = src_hook.get_conn()
    src_conn.autocommit(True)
    src_cur = src_conn.cursor()

    tgt_conn = tgt_hook.get_conn()
    tgt_conn.autocommit = True
    tgt_cur = tgt_conn.cursor()

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

            # Map SQL Server type to PostgreSQL type
            pg_type = map_sqlserver_to_postgres_type(data_type, char_len, num_prec, num_scale)

            # Build column definition
            col_def = f'"{col_name}" {pg_type}'

            # Add GENERATED AS IDENTITY if source column is identity
            if is_identity:
                col_def += " GENERATED ALWAYS AS IDENTITY"

            # Add NULL/NOT NULL
            # Special case: Allow NULL for text columns even if source says NOT NULL
            # to handle empty strings that get converted to NULL during CSV export
            if is_nullable == 'NO' and pg_type not in ('TEXT', 'VARCHAR') and not pg_type.startswith('VARCHAR('):
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
            pk_def = f"PRIMARY KEY ({', '.join(f'"{col}"' for col in pk_cols)})"
            col_defs.append(pk_def)

        # Create table on target
        create_sql = f'CREATE TABLE "{table}" (\n  {",\n  ".join(col_defs)}\n);'
        tgt_cur.execute(create_sql)
        log.info(f"Created table {table}")

    src_cur.close()
    src_conn.close()
    tgt_cur.close()
    tgt_conn.close()

    log.info("Target schema created successfully")


def copy_table_src_to_tgt(table: str) -> None:
    """
    Copy table from SQL Server source to PostgreSQL target.

    Strategy: Read from source via SELECT, write to target via CSV bulk insert.
    Uses memory-capped streaming with SpooledTemporaryFile.
    """
    src_hook = MsSqlHook(mssql_conn_id=SRC_CONN_ID)
    tgt_hook = PostgresHook(postgres_conn_id=TGT_CONN_ID, schema='stackoverflow_target')

    log.info(
        "[%s] starting buffered copy (memory cap≈%.1f MB)",
        table,
        SPOOLED_MAX_MEMORY_BYTES / (1024 * 1024),
    )

    # Get target table columns
    with tgt_hook.get_conn() as tgt_conn_tmp:
        tgt_conn_tmp.autocommit = True
        with tgt_conn_tmp.cursor() as tgt_cur_tmp:
            tgt_cur_tmp.execute(
                """
                SELECT c.column_name, c.is_nullable, ident.is_identity
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT column_name, 'YES' as is_identity
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                      AND column_default LIKE 'nextval%%'
                ) ident ON c.column_name = ident.column_name
                WHERE c.table_schema = 'public'
                  AND c.table_name = %s
                ORDER BY c.ordinal_position
                """,
                (table, table),
            )
            column_info = tgt_cur_tmp.fetchall()
            target_columns = [row[0] for row in column_info]
            nullable_columns = {row[0] for row in column_info if row[1] == 'YES'}
            identity_columns = {row[0] for row in column_info if row[2] == 'YES'}

    if not target_columns:
        raise RuntimeError(f"Target table {table} has no columns")

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
                        elif isinstance(v, datetime):
                            # Format datetime as ISO string without microseconds
                            csv_row.append(v.strftime('%Y-%m-%d %H:%M:%S'))
                        elif isinstance(v, bool):
                            # PostgreSQL expects 't'/'f' or 'true'/'false' for boolean
                            csv_row.append('t' if v else 'f')
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

        # Write to PostgreSQL target using COPY command for better performance
        tgt_conn = tgt_hook.get_conn()
        tgt_conn.autocommit = False
        tgt_cur = tgt_conn.cursor()

        try:
            # Verify target table exists
            tgt_cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table,),
            )
            if tgt_cur.fetchone() is None:
                raise RuntimeError(
                    f"Target table {table} does not exist; did schema creation run?"
                )

            # Delete all rows from target table
            tgt_cur.execute(f'DELETE FROM "{table}"')

            # Disable foreign key checks for this table
            tgt_cur.execute(f'ALTER TABLE "{table}" DISABLE TRIGGER ALL')

            # Build column list (excluding identity columns for COPY)
            non_identity_columns = [col for col in target_columns if col not in identity_columns]

            # If table has identity columns, we need to override them
            # PostgreSQL allows inserting into GENERATED ALWAYS columns if we use OVERRIDING SYSTEM VALUE
            if identity_columns:
                column_list = ", ".join(f'"{col}"' for col in target_columns)
                copy_sql = f'COPY "{table}" ({column_list}) FROM STDIN WITH (FORMAT CSV, NULL \'\')'

                # We need to enable identity insert equivalent
                # In PostgreSQL, we'll drop and recreate the identity temporarily
                for id_col in identity_columns:
                    # Get the sequence name
                    tgt_cur.execute(f"""
                        SELECT pg_get_serial_sequence('{table}', '{id_col}')
                    """)
                    seq_result = tgt_cur.fetchone()
                    if seq_result and seq_result[0]:
                        # Temporarily allow manual values by altering the column
                        tgt_cur.execute(f'ALTER TABLE "{table}" ALTER COLUMN "{id_col}" DROP IDENTITY IF EXISTS')
            else:
                column_list = ", ".join(f'"{col}"' for col in target_columns)
                copy_sql = f'COPY "{table}" ({column_list}) FROM STDIN WITH (FORMAT CSV, NULL \'\')'

            # Use PostgreSQL's COPY command for bulk loading
            tgt_cur.copy_expert(copy_sql, spool)

            # Re-add identity if we removed it
            if identity_columns:
                for id_col in identity_columns:
                    tgt_cur.execute(f'ALTER TABLE "{table}" ALTER COLUMN "{id_col}" ADD GENERATED ALWAYS AS IDENTITY')

            # Re-enable foreign key checks
            tgt_cur.execute(f'ALTER TABLE "{table}" ENABLE TRIGGER ALL')

            # Get row count
            tgt_cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            row_count = tgt_cur.fetchone()[0]

            tgt_conn.commit()
            log.info("[%s] copy completed (bytes=%s, rows=%s)", table, written_bytes, row_count)
        except Exception as e:
            tgt_conn.rollback()
            log.error(f"[{table}] copy failed: {e}")
            raise
        finally:
            tgt_cur.close()
            tgt_conn.close()


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
        # Check if table exists (use exact case match)
        cur.execute(
            'SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s',
            ('public', table)
        )
        if cur.fetchone() is None:
            log.info(f"Table {table} not found, skipping sequence alignment")
            continue

        # Get max ID (use quoted identifiers for case-sensitive table/column names)
        cur.execute(f'SELECT COALESCE(MAX("{pk}"), 0) FROM "{table}"')
        (max_id,) = cur.fetchone()

        if max_id > 0:
            # Get the sequence name for this identity column
            # pg_get_serial_sequence requires properly quoted table name
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
    dag_id="replicate_stackoverflow_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["stackoverflow", "postgres", "replication", "sql-server-to-postgres"],
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
