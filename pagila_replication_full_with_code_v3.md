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

---

## 3. Start Source and Target Postgres Containers (no Docker Compose)
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

Start **target** on host port **5444**:
```bash
docker run -d \
  --name pagila-pg-target \
  -e POSTGRES_DB=pagila \
  -e POSTGRES_USER=pagila_tgt \
  -e POSTGRES_PASSWORD=pagila_tgt_pw \
  -p 5444:5432 \
  postgres:16
```

---

## 4. Create Airflow Connections
You can use `astro dev run connections ...` (note: with some Astro versions the `airflow` keyword is implicit).

**Source connection (to the container above on 5433):**
```bash
astro dev run connections add pagila_postgres \
  --conn-type postgres \
  --conn-host host.docker.internal \
  --conn-port 5433 \
  --conn-login pagila \
  --conn-password pagila_pw \
  --conn-schema pagila
```

**Target connection (to the container above on 5444):**
```bash
astro dev run connections add pagila_tgt \
  --conn-type postgres \
  --conn-host host.docker.internal \
  --conn-port 5444 \
  --conn-login pagila_tgt \
  --conn-password pagila_tgt_pw \
  --conn-schema pagila
```

> If your source DB is reachable via the Astro network as a service called `postgres` on port `5432`, use `--conn-host postgres --conn-port 5432` instead.

---

## 5. Place SQL Files
Place the Pagila SQL files in your Astro project at:
```
include/pagila/schema.sql
include/pagila/data.sql
```
Ensure `data.sql` uses `COPY ... FROM stdin;` blocks with a terminating `\.` line for each section (download a **raw** file from the official repo).

---

## 6. Full DAG: Idempotent Pagila Loader (Source)

**File:** `dags/load_pagila_dag.py`

```python
from __future__ import annotations
from datetime import datetime
import hashlib

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "pagila_postgres"
SCHEMA_SQL_PATH = "/usr/local/airflow/include/pagila/schema.sql"
DATA_SQL_PATH   = "/usr/local/airflow/include/pagila/data.sql"

# Toggle this off if your schema/data do NOT contain 'OWNER TO postgres'
ENSURE_POSTGRES_ROLE = True

def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def compute_input_fingerprints(**context) -> None:
    schema_hash = file_sha256(SCHEMA_SQL_PATH)
    data_hash   = file_sha256(DATA_SQL_PATH)
    ti = context["ti"]
    ti.xcom_push(key="schema_hash", value=schema_hash)
    ti.xcom_push(key="data_hash",   value=data_hash)

def should_reload(**context) -> bool:
    ti = context["ti"]
    schema_hash = ti.xcom_pull(key="schema_hash", task_ids="compute_hashes")
    data_hash   = ti.xcom_pull(key="data_hash",   task_ids="compute_hashes")
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
        locked, = cur.fetchone()
        if not locked:
            raise RuntimeError("Could not obtain advisory lock; another run holds it.")

def release_lock() -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock_all();")

def load_pagila_copy_blocks() -> None:
    import io
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
                    if line.strip() == "\.":
                        import io as _io
                        with _io.BytesIO(bytes(buf)) as fp:
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
    data_hash   = ti.xcom_pull(key="data_hash",   task_ids="compute_hashes")
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM actor;");    actor = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM film;");     film  = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM customer;"); cust  = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM rental;");   rent  = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO pagila_load_audit
              (schema_sha256, data_sha256, actor_count, film_count, customer_count, rental_count, succeeded)
            VALUES (%s,%s,%s,%s,%s,%s, TRUE);
            """
            , (schema_hash, data_hash, actor, film, cust, rent)
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

---

## 7. Full DAG: Pagila Replication (Source → Target)

**File:** `dags/replicate_pagila_to_target.py`

```python
from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Use your actual source connection id here if different
SRC_CONN_ID = "pagila_postgres"
TGT_CONN_ID = "pagila_tgt"

SCHEMA_SQL_PATH = "/usr/local/airflow/include/pagila/schema.sql"
ENSURE_POSTGRES_ROLE_ON_TARGET = True

# FK-safe order for Pagila
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

def ensure_tgt_roles():
    hook = PostgresHook(postgres_conn_id=TGT_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='postgres') THEN
            CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'postgres_pw';
          END IF;
        END$$;
        """)

def reset_target_schema():
    hook = PostgresHook(postgres_conn_id=TGT_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name='public') THEN
            EXECUTE 'DROP SCHEMA public CASCADE';
          END IF;
          EXECUTE 'CREATE SCHEMA public AUTHORIZATION current_user';
          PERFORM set_config('search_path', 'public', true);
        END$$;
        """)

def copy_table_src_to_tgt(table: str):
    """Robust copy for partitioned and non-partitioned tables.
    COPY (SELECT * FROM public.table) TO STDOUT on source; COPY public.table FROM STDIN on target.
    Assumes identical schemas (target created from schema.sql).
    """
    import io
    src = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    tgt = PostgresHook(postgres_conn_id=TGT_CONN_ID)
    with src.get_conn() as src_conn, tgt.get_conn() as tgt_conn:
        src_conn.autocommit = True
        tgt_conn.autocommit = True
        s_cur = src_conn.cursor()
        t_cur = tgt_conn.cursor()

        # Ensure target table exists
        t_cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name=%s
            """, (table,),
        )
        if t_cur.fetchone() is None:
            raise RuntimeError(f"Target table public.{table} does not exist; did schema.sql run?")

        copy_out_query = f"COPY (SELECT * FROM public.{table}) TO STDOUT"
        copy_in_cmd    = f"COPY public.{table} FROM STDIN"

        buf = io.BytesIO()
        s_cur.copy_expert(copy_out_query, buf)
        buf.seek(0)
        t_cur.copy_expert(copy_in_cmd, buf)

        s_cur.close()
        t_cur.close()

def set_identity_sequences():
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

    copy_tasks = []
    prev = create_target_schema
    for tbl in TABLE_ORDER:
        t = PythonOperator(
            task_id=f"copy_{tbl}",
            python_callable=copy_table_src_to_tgt,
            op_kwargs={"table": tbl},
        )
        copy_tasks.append(t)
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
```

---

## 8. Running the DAGs
```bash
astro dev start

# Load source
astro dev run dags trigger load_pagila_to_postgres

# Replicate to target
astro dev run dags trigger replicate_pagila_to_target
```

---

## 9. Verification & QA
```bash
# Row counts on target (examples)
docker exec -it pagila-pg-target psql -U pagila_tgt -d pagila -c "SELECT COUNT(*) FROM actor;"
docker exec -it pagila-pg-target psql -U pagila_tgt -d pagila -c "SELECT COUNT(*) FROM rental;"
```
For a quick smoke test, counts on target should match source.

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
│       ├── schema.sql
│       └── data.sql
├── requirements.txt
└── .env   (optional for AIRFLOW_CONN_... variables)
```
