from __future__ import annotations

from datetime import datetime
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

SRC_CONN_ID = "pagila_postgres"
TGT_CONN_ID = "pagila_tgt"

ENSURE_POSTGRES_ROLE_ON_TARGET = True

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
    "store",
    "customer",
    "staff",
    "inventory",
    "rental",
    "payment",
]


def ensure_tgt_roles() -> None:
    hook = PostgresHook(postgres_conn_id=TGT_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='postgres') THEN
            CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'postgres_pw';
          END IF;
        END$$;
        """
        )


def reset_target_schema() -> None:
    hook = PostgresHook(postgres_conn_id=TGT_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name='public') THEN
            EXECUTE 'DROP SCHEMA public CASCADE';
          END IF;
          EXECUTE 'CREATE SCHEMA public AUTHORIZATION current_user';
          PERFORM set_config('search_path', 'public', true);
        END$$;
        """
        )


def copy_table_src_to_tgt(table: str) -> None:
    """Safely copy a table from source to target using COPY streaming."""
    src = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    tgt = PostgresHook(postgres_conn_id=TGT_CONN_ID)
    with src.get_conn() as src_conn, tgt.get_conn() as tgt_conn:
        src_conn.autocommit = True
        tgt_conn.autocommit = True
        s_cur = src_conn.cursor()
        t_cur = tgt_conn.cursor()

        t_cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name=%s
            """,
            (table,),
        )
        if t_cur.fetchone() is None:
            raise RuntimeError(
                f"Target table public.{table} does not exist; did schema.sql run?"
            )

        copy_out_query = f"COPY (SELECT * FROM public.{table}) TO STDOUT"
        copy_in_cmd = f"COPY public.{table} FROM STDIN"

        buf = BytesIO()
        s_cur.copy_expert(copy_out_query, buf)
        buf.seek(0)
        t_cur.copy_expert(copy_in_cmd, buf)

        s_cur.close()
        t_cur.close()


def set_identity_sequences() -> None:
    tgt = PostgresHook(postgres_conn_id=TGT_CONN_ID)
    with tgt.get_conn() as conn, conn.cursor() as cur:
        seq_fix_sql = [
            ("actor", "actor_id", "actor_actor_id_seq"),
            ("category", "category_id", "category_category_id_seq"),
            ("film", "film_id", "film_film_id_seq"),
            ("customer", "customer_id", "customer_customer_id_seq"),
            ("staff", "staff_id", "staff_staff_id_seq"),
            ("store", "store_id", "store_store_id_seq"),
            ("inventory", "inventory_id", "inventory_inventory_id_seq"),
            ("rental", "rental_id", "rental_rental_id_seq"),
            ("payment", "payment_id", "payment_payment_id_seq"),
            ("city", "city_id", "city_city_id_seq"),
            ("address", "address_id", "address_address_id_seq"),
            ("country", "country_id", "country_country_id_seq"),
        ]
        for table, pk, seq in seq_fix_sql:
            cur.execute(f"SELECT COALESCE(MAX({pk}), 0) FROM public.{table};")
            (max_id,) = cur.fetchone()
            cur.execute("SELECT setval(%s, %s, %s);", (seq, max_id, True))


with DAG(
    dag_id="replicate_pagila_to_target",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pagila", "postgres", "replication"],
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

    create_target_schema = SQLExecuteQueryOperator(
        task_id="create_target_schema",
        conn_id=TGT_CONN_ID,
        sql="pagila/schema.sql",
    )

    prev = create_target_schema
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
        reset_tgt >> ensure_tgt_role >> create_target_schema
    else:
        reset_tgt >> create_target_schema

    prev >> fix_sequences
