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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pagila_load_audit (
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
            """
            SELECT succeeded
            FROM pagila_load_audit
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
        cur.execute("SELECT COUNT(*) FROM actor;")
        actor = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM film;")
        film = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM customer;")
        cust = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM rental;")
        rent = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO pagila_load_audit
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
        """,
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
        """,
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
