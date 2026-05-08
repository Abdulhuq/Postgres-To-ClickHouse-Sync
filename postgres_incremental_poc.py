"""
PostgreSQL incremental sync POC using trigger queue + LISTEN/NOTIFY.

This script demonstrates a materialized-view-style approach where:
1) Source tables a, b, c, d, e emit row-change events into change_triggers.
2) A Python listener subscribes to a Postgres channel via LISTEN/NOTIFY.
3) Only impacted joined rows are recomputed and upserted into mv_joined.

Usage examples:
  python postgres_incremental_poc.py setup
  python postgres_incremental_poc.py seed --a-count 1000 --children-per-parent 2
  python postgres_incremental_poc.py rebuild
  python postgres_incremental_poc.py listen
  python postgres_incremental_poc.py simulate --iterations 50 --sleep 0.5
  python postgres_incremental_poc.py status

Connection:
  Set PG_DSN if needed, e.g.
  set PG_DSN=host=localhost port=5432 dbname=testdb user=postgres password=root
"""

from __future__ import annotations

import argparse
import random
import select
import time
from datetime import datetime
from typing import Iterable, List, Sequence, Set

import psycopg2
import psycopg2.extras

CHANNEL_NAME = "change_triggers_channel"
PG_DSN = "host=localhost port=5433 dbname=testdb user=postgres password=jSwZFEJqxuC6hquJ*Vu"


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def connect(autocommit: bool = False):
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = autocommit
    return conn


def setup_schema() -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS a (
        id BIGSERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS b (
        id BIGSERIAL PRIMARY KEY,
        a_id BIGINT NOT NULL REFERENCES a(id) ON DELETE CASCADE,
        value_b TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS c (
        id BIGSERIAL PRIMARY KEY,
        b_id BIGINT NOT NULL REFERENCES b(id) ON DELETE CASCADE,
        value_c TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS d (
        id BIGSERIAL PRIMARY KEY,
        c_id BIGINT NOT NULL REFERENCES c(id) ON DELETE CASCADE,
        value_d TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS e (
        id BIGSERIAL PRIMARY KEY,
        d_id BIGINT NOT NULL REFERENCES d(id) ON DELETE CASCADE,
        value_e TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS change_triggers (
        id BIGSERIAL PRIMARY KEY,
        source_table TEXT NOT NULL CHECK (source_table IN ('a', 'b', 'c', 'd', 'e')),
        pk_id BIGINT NOT NULL,
        op TEXT NOT NULL CHECK (op IN ('INSERT', 'UPDATE')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        processed_at TIMESTAMPTZ,
        error TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_change_triggers_unprocessed
    ON change_triggers (processed_at, id)
    WHERE processed_at IS NULL;

    CREATE TABLE IF NOT EXISTS mv_joined (
        e_id BIGINT PRIMARY KEY,
        d_id BIGINT NOT NULL,
        c_id BIGINT NOT NULL,
        b_id BIGINT NOT NULL,
        a_id BIGINT NOT NULL,
        a_name TEXT NOT NULL,
        value_b TEXT NOT NULL,
        value_c TEXT NOT NULL,
        value_d TEXT NOT NULL,
        value_e TEXT NOT NULL,
        source_updated_at TIMESTAMPTZ NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE OR REPLACE FUNCTION enqueue_change_event() RETURNS trigger AS $$
    DECLARE
        changed_id BIGINT;
    BEGIN
        changed_id := NEW.id;

        INSERT INTO change_triggers (source_table, pk_id, op)
        VALUES (TG_TABLE_NAME, changed_id, TG_OP);

        PERFORM pg_notify('{CHANNEL_NAME}', TG_TABLE_NAME || ':' || changed_id::TEXT || ':' || TG_OP);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS trg_a_change ON a;
    DROP TRIGGER IF EXISTS trg_b_change ON b;
    DROP TRIGGER IF EXISTS trg_c_change ON c;
    DROP TRIGGER IF EXISTS trg_d_change ON d;
    DROP TRIGGER IF EXISTS trg_e_change ON e;

    CREATE TRIGGER trg_a_change
    AFTER INSERT OR UPDATE ON a
    FOR EACH ROW EXECUTE FUNCTION enqueue_change_event();

    CREATE TRIGGER trg_b_change
    AFTER INSERT OR UPDATE ON b
    FOR EACH ROW EXECUTE FUNCTION enqueue_change_event();

    CREATE TRIGGER trg_c_change
    AFTER INSERT OR UPDATE ON c
    FOR EACH ROW EXECUTE FUNCTION enqueue_change_event();

    CREATE TRIGGER trg_d_change
    AFTER INSERT OR UPDATE ON d
    FOR EACH ROW EXECUTE FUNCTION enqueue_change_event();

    CREATE TRIGGER trg_e_change
    AFTER INSERT OR UPDATE ON e
    FOR EACH ROW EXECUTE FUNCTION enqueue_change_event();
    """

    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        log("Schema, trigger queue, and LISTEN/NOTIFY triggers created.")
    finally:
        conn.close()


def seed_data(a_count: int, children_per_parent: int) -> None:
    conn = connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            log(f"Seeding a={a_count}, children_per_parent={children_per_parent}...")

            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO a (name) VALUES %s RETURNING id",
                [(f"a_{i}",) for i in range(a_count)],
                page_size=5000,
            )
            a_ids = [row[0] for row in cur.fetchall()]

            b_rows = []
            for a_id in a_ids:
                for i in range(children_per_parent):
                    b_rows.append((a_id, f"b_{a_id}_{i}"))
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO b (a_id, value_b) VALUES %s RETURNING id, a_id",
                b_rows,
                page_size=5000,
            )
            b_ids = [row[0] for row in cur.fetchall()]

            c_rows = [(b_id, f"c_{b_id}") for b_id in b_ids]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO c (b_id, value_c) VALUES %s RETURNING id",
                c_rows,
                page_size=5000,
            )
            c_ids = [row[0] for row in cur.fetchall()]

            d_rows = [(c_id, f"d_{c_id}") for c_id in c_ids]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO d (c_id, value_d) VALUES %s RETURNING id",
                d_rows,
                page_size=5000,
            )
            d_ids = [row[0] for row in cur.fetchall()]

            e_rows = [(d_id, f"e_{d_id}") for d_id in d_ids]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO e (d_id, value_e) VALUES %s",
                e_rows,
                page_size=5000,
            )

        conn.commit()
        log("Seed completed.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def rebuild_mv() -> None:
    conn = connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE mv_joined")
            cur.execute(
                """
                INSERT INTO mv_joined (
                    e_id, d_id, c_id, b_id, a_id,
                    a_name, value_b, value_c, value_d, value_e,
                    source_updated_at, synced_at
                )
                SELECT
                    e.id,
                    d.id,
                    c.id,
                    b.id,
                    a.id,
                    a.name,
                    b.value_b,
                    c.value_c,
                    d.value_d,
                    e.value_e,
                    GREATEST(a.updated_at, b.updated_at, c.updated_at, d.updated_at, e.updated_at),
                    now()
                FROM e
                JOIN d ON d.id = e.d_id
                JOIN c ON c.id = d.c_id
                JOIN b ON b.id = c.b_id
                JOIN a ON a.id = b.a_id
                """
            )
        conn.commit()
        log("Full rebuild of mv_joined completed.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def impacted_e_ids(cur, source_table: str, pk_id: int) -> Set[int]:
    if source_table == "a":
        cur.execute(
            """
            SELECT e.id
            FROM e
            JOIN d ON d.id = e.d_id
            JOIN c ON c.id = d.c_id
            JOIN b ON b.id = c.b_id
            WHERE b.a_id = %s
            """,
            (pk_id,),
        )
    elif source_table == "b":
        cur.execute(
            """
            SELECT e.id
            FROM e
            JOIN d ON d.id = e.d_id
            JOIN c ON c.id = d.c_id
            WHERE c.b_id = %s
            """,
            (pk_id,),
        )
    elif source_table == "c":
        cur.execute(
            """
            SELECT e.id
            FROM e
            JOIN d ON d.id = e.d_id
            WHERE d.c_id = %s
            """,
            (pk_id,),
        )
    elif source_table == "d":
        cur.execute("SELECT id FROM e WHERE d_id = %s", (pk_id,))
    elif source_table == "e":
        cur.execute("SELECT id FROM e WHERE id = %s", (pk_id,))
    else:
        return set()

    return {row[0] for row in cur.fetchall()}


def upsert_mv_rows(cur, e_ids: Sequence[int]) -> int:
    if not e_ids:
        return 0

    cur.execute(
        """
        INSERT INTO mv_joined (
            e_id, d_id, c_id, b_id, a_id,
            a_name, value_b, value_c, value_d, value_e,
            source_updated_at, synced_at
        )
        SELECT
            e.id,
            d.id,
            c.id,
            b.id,
            a.id,
            a.name,
            b.value_b,
            c.value_c,
            d.value_d,
            e.value_e,
            GREATEST(a.updated_at, b.updated_at, c.updated_at, d.updated_at, e.updated_at),
            now()
        FROM e
        JOIN d ON d.id = e.d_id
        JOIN c ON c.id = d.c_id
        JOIN b ON b.id = c.b_id
        JOIN a ON a.id = b.a_id
        WHERE e.id = ANY(%s)
        ON CONFLICT (e_id) DO UPDATE
        SET
            d_id = EXCLUDED.d_id,
            c_id = EXCLUDED.c_id,
            b_id = EXCLUDED.b_id,
            a_id = EXCLUDED.a_id,
            a_name = EXCLUDED.a_name,
            value_b = EXCLUDED.value_b,
            value_c = EXCLUDED.value_c,
            value_d = EXCLUDED.value_d,
            value_e = EXCLUDED.value_e,
            source_updated_at = EXCLUDED.source_updated_at,
            synced_at = EXCLUDED.synced_at
        """,
        (list(e_ids),),
    )
    return len(e_ids)


def process_change_queue(batch_size: int = 1000) -> int:
    conn = connect(autocommit=False)
    processed_count = 0

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, source_table, pk_id, op
                FROM change_triggers
                WHERE processed_at IS NULL
                ORDER BY id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
                """,
                (batch_size,),
            )
            rows = cur.fetchall()

            if not rows:
                conn.commit()
                return 0

            all_impacted: Set[int] = set()
            trigger_ids: List[int] = []

            for trigger_id, source_table, pk_id, _op in rows:
                trigger_ids.append(trigger_id)
                impacted = impacted_e_ids(cur, source_table, pk_id)
                all_impacted.update(impacted)

            upserted = upsert_mv_rows(cur, sorted(all_impacted))

            cur.execute(
                "UPDATE change_triggers SET processed_at = now(), error = NULL WHERE id = ANY(%s)",
                (trigger_ids,),
            )

            processed_count = len(rows)
            conn.commit()
            log(
                f"Processed {processed_count} queue events; upserted {upserted} impacted mv rows."
            )
            return processed_count

    except Exception as exc:
        conn.rollback()
        log(f"Queue processing failed: {exc}")
        raise
    finally:
        conn.close()


def listen_loop(poll_timeout_seconds: int = 10) -> None:
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"LISTEN {CHANNEL_NAME};")
        log(f"Listening on channel '{CHANNEL_NAME}'...")

        while True:
            # Wait for notifications, then drain queue in batches.
            ready, _, _ = select.select([conn], [], [], poll_timeout_seconds)
            if ready:
                conn.poll()
                while conn.notifies:
                    _ = conn.notifies.pop(0)

            while True:
                processed = process_change_queue(batch_size=1000)
                if processed == 0:
                    break

    except KeyboardInterrupt:
        log("Listener stopped by user.")
    finally:
        conn.close()


def simulate_changes(iterations: int, sleep_seconds: float) -> None:
    conn = connect(autocommit=False)
    random.seed(42)

    try:
        with conn.cursor() as cur:
            for i in range(iterations):
                table = random.choice(["a", "b", "c", "d", "e"])
                cur.execute(f"SELECT id FROM {table} ORDER BY random() LIMIT 1")
                row = cur.fetchone()
                if not row:
                    log(f"No rows in {table}; skipping iteration {i + 1}/{iterations}")
                    continue

                row_id = row[0]
                if table == "a":
                    cur.execute(
                        "UPDATE a SET name = %s, updated_at = now() WHERE id = %s",
                        (f"a_{row_id}_u{i}", row_id),
                    )
                elif table == "b":
                    cur.execute(
                        "UPDATE b SET value_b = %s, updated_at = now() WHERE id = %s",
                        (f"b_{row_id}_u{i}", row_id),
                    )
                elif table == "c":
                    cur.execute(
                        "UPDATE c SET value_c = %s, updated_at = now() WHERE id = %s",
                        (f"c_{row_id}_u{i}", row_id),
                    )
                elif table == "d":
                    cur.execute(
                        "UPDATE d SET value_d = %s, updated_at = now() WHERE id = %s",
                        (f"d_{row_id}_u{i}", row_id),
                    )
                else:
                    cur.execute(
                        "UPDATE e SET value_e = %s, updated_at = now() WHERE id = %s",
                        (f"e_{row_id}_u{i}", row_id),
                    )

                conn.commit()
                log(f"Simulated change {i + 1}/{iterations}: {table}.id={row_id}")
                time.sleep(sleep_seconds)
    finally:
        conn.close()


def status() -> None:
    conn = connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            for table in ["a", "b", "c", "d", "e", "mv_joined"]:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                log(f"{table}: {count}")

            cur.execute("SELECT COUNT(*) FROM change_triggers WHERE processed_at IS NULL")
            pending = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM change_triggers")
            total = cur.fetchone()[0]
            log(f"change_triggers: total={total}, pending={pending}")
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incremental LISTEN/NOTIFY POC for PostgreSQL")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("setup", help="Create tables, queue, trigger function and triggers")

    seed = sub.add_parser("seed", help="Seed synthetic hierarchical data")
    seed.add_argument("--a-count", type=int, default=1000, help="Number of rows in table a")
    seed.add_argument(
        "--children-per-parent",
        type=int,
        default=2,
        help="Number of b rows per a row (and one row down each level afterward)",
    )

    sub.add_parser("rebuild", help="Full rebuild of mv_joined")
    sub.add_parser("listen", help="Run LISTEN/NOTIFY consumer and process queue")

    sim = sub.add_parser("simulate", help="Generate random updates to a..e")
    sim.add_argument("--iterations", type=int, default=20)
    sim.add_argument("--sleep", type=float, default=0.25)

    sub.add_parser("status", help="Show row counts and queue backlog")
    return parser.parse_args()


def main() -> None:
    global PG_DSN

    args = parse_args()

    if "PG_DSN" in __import__("os").environ:
        PG_DSN = __import__("os").environ["PG_DSN"]

    if args.command == "setup":
        setup_schema()
    elif args.command == "seed":
        seed_data(a_count=args.a_count, children_per_parent=args.children_per_parent)
    elif args.command == "rebuild":
        rebuild_mv()
    elif args.command == "listen":
        listen_loop()
    elif args.command == "simulate":
        simulate_changes(iterations=args.iterations, sleep_seconds=args.sleep)
    elif args.command == "status":
        status()


if __name__ == "__main__":
    main()
