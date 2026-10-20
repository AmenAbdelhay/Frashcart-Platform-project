"""
Light daily Production -> Bronze sync.

Per table it only moves what is needed:
  1) NEW / MISSING rows (any age): read ONLY the primary keys from the source (tiny),
     compare with the warehouse, then fetch from the source just the rows whose key is missing.
  2) CHANGED rows: if the table has `updated_at`, fetch only rows with
     updated_at >= (max updated_at already in the warehouse - LOOKBACK).

Everything is UPSERTed by primary key (unique index), so re-running never creates duplicates.
One transaction per table: on failure that table is left unchanged.
Rows deleted in the source are not removed from the warehouse.
"""
import os
import logging
import tempfile
from datetime import timedelta
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SOURCE = {
    "host": os.getenv("PROD_HOST", "localhost"),
    "port": int(os.getenv("PROD_PORT", "5433")),
    "dbname": os.getenv("PROD_DB", "freshcart_source"),
    "user": os.getenv("PROD_USER", "analytics_reader"),
    "password": os.getenv("PROD_PASSWORD"),
}
WAREHOUSE = {
    "host": os.getenv("WAREHOUSE_HOST", "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", "5435")),
    "dbname": os.getenv("WAREHOUSE_DB", "postgres"),
    "user": os.getenv("WAREHOUSE_USER", "fresh_wh"),
    "password": os.getenv("WAREHOUSE_PASSWORD"),
}

SOURCE_SCHEMA = "app"
TARGET_SCHEMA = "bronze_app"
TABLES = [
    "stores", "customers", "products", "product_prices",
    "orders", "order_items", "payments",
]

UPDATED_COL = "updated_at"           # column used to detect changed rows
LOOKBACK = timedelta(hours=1)        # safety overlap for the watermark (harmless: upsert is idempotent)
KEY_BATCH = 5000                     # missing keys fetched per query

ident = sql.Identifier


def column_defs(conn, schema, table):
    """[(column, exact source type)] - enums/domains/composites become text."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.attname,
                   CASE WHEN t.typtype IN ('e','d','c') THEN 'text'
                        WHEN t.typcategory = 'A' AND et.typtype IN ('e','d','c') THEN 'text[]'
                        ELSE format_type(a.atttypid, a.atttypmod) END
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            LEFT JOIN pg_type et ON t.typcategory = 'A' AND et.oid = t.typelem
            WHERE n.nspname = %s AND c.relname = %s
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
        """, (schema, table))
        return cur.fetchall()


def primary_key(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
            WHERE i.indrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass
              AND i.indisprimary
            ORDER BY array_position(i.indkey::int2[], a.attnum)
        """, (schema, table))
        return [r[0] for r in cur.fetchall()]


def src_copy_to_buf(src, query, params, buf):
    """Run COPY (query) TO STDOUT on the source into `buf` (a disk-backed temp file)."""
    with src.cursor() as s:
        stmt = "COPY (" + query.as_string(src) + ") TO STDOUT"
        s.copy_expert(s.mogrify(stmt, params).decode(), buf)


def sync_table(src, wh, table):
    defs = column_defs(src, SOURCE_SCHEMA, table)
    if not defs:
        raise ValueError(f"Source table {SOURCE_SCHEMA}.{table} not found")
    pk = primary_key(src, SOURCE_SCHEMA, table)
    if not pk:
        raise ValueError(f"{SOURCE_SCHEMA}.{table} has no primary key (needed to avoid duplicates)")

    types = dict(defs)
    names = [n for n, _ in defs]
    non_pk = [n for n in names if n not in pk]
    cols = sql.SQL(", ").join(ident(n) for n in names)
    pk_cols = sql.SQL(", ").join(ident(n) for n in pk)
    src_tbl = sql.SQL("{}.{}").format(ident(SOURCE_SCHEMA), ident(table))
    target = sql.SQL("{}.{}").format(ident(TARGET_SCHEMA), ident(table))
    ddl = sql.SQL(", ").join(sql.SQL("{} {}").format(ident(n), sql.SQL(t)) for n, t in defs)
    has_updated = UPDATED_COL in types

    with wh.cursor() as cur:
        # --- target table: create if missing, add new source columns, unique index on PK
        cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(target, ddl))
        for n, t in defs:
            cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                target, ident(n), sql.SQL(t)))
        cur.execute(sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})").format(
            ident(f"ux_{table}_pk"), target, pk_cols))

        cur.execute(sql.SQL("CREATE TEMP TABLE _stg (LIKE {}) ON COMMIT DROP").format(target))

        # --- (1) MISSING rows: compare primary keys only
        cur.execute(sql.SQL(
            "CREATE TEMP TABLE _src_keys ON COMMIT DROP AS SELECT {} FROM {} WITH NO DATA"
        ).format(pk_cols, target))
        with tempfile.TemporaryFile() as buf:
            src_copy_to_buf(src, sql.SQL("SELECT {} FROM {}").format(pk_cols, src_tbl), None, buf)
            buf.seek(0)
            cur.copy_expert(sql.SQL("COPY _src_keys ({}) FROM STDIN").format(pk_cols).as_string(wh), buf)

        join_on = sql.SQL(" AND ").join(sql.SQL("t.{c} = k.{c}").format(c=ident(n)) for n in pk)
        cur.execute(sql.SQL(
            "SELECT {kc} FROM _src_keys k WHERE NOT EXISTS (SELECT 1 FROM {t} t WHERE {j})"
        ).format(kc=sql.SQL(", ").join(sql.SQL("k.{}").format(ident(n)) for n in pk),
                 t=target, j=join_on))
        missing = cur.fetchall()

        # fetch only the missing rows from the source, in batches
        unnest = sql.SQL("unnest({})").format(sql.SQL(", ").join(
            sql.SQL("%s::{}[]").format(sql.SQL(types[n])) for n in pk))
        by_key = sql.SQL("SELECT {c} FROM {s} WHERE ({pk}) IN (SELECT * FROM {u})").format(
            c=cols, s=src_tbl, pk=pk_cols, u=unnest)
        for i in range(0, len(missing), KEY_BATCH):
            batch = missing[i:i + KEY_BATCH]
            arrays = [list(col) for col in zip(*batch)]
            with tempfile.TemporaryFile() as buf:
                src_copy_to_buf(src, by_key, arrays, buf)
                buf.seek(0)
                cur.copy_expert(sql.SQL("COPY _stg ({}) FROM STDIN").format(cols).as_string(wh), buf)

        # --- (2) CHANGED rows: only rows touched since the warehouse watermark
        if has_updated:
            cur.execute(sql.SQL("SELECT max({}) FROM {}").format(ident(UPDATED_COL), target))
            watermark = cur.fetchone()[0]
            if watermark is not None:
                with tempfile.TemporaryFile() as buf:
                    src_copy_to_buf(
                        src,
                        sql.SQL("SELECT {} FROM {} WHERE {} >= %s").format(cols, src_tbl, ident(UPDATED_COL)),
                        (watermark - LOOKBACK,), buf)
                    buf.seek(0)
                    cur.copy_expert(sql.SQL("COPY _stg ({}) FROM STDIN").format(cols).as_string(wh), buf)

        # --- upsert (DISTINCT ON removes overlap between the two fetches)
        if non_pk:
            action = sql.SQL("DO UPDATE SET {}").format(sql.SQL(", ").join(
                sql.SQL("{c} = EXCLUDED.{c}").format(c=ident(n)) for n in non_pk))
        else:
            action = sql.SQL("DO NOTHING")
        cur.execute(sql.SQL(
            "WITH up AS (INSERT INTO {t} ({c}) SELECT DISTINCT ON ({pk}) {c} FROM _stg ORDER BY {pk} "
            "ON CONFLICT ({pk}) {a} RETURNING (xmax = 0) AS ins) "
            "SELECT count(*) FILTER (WHERE ins), count(*) FILTER (WHERE NOT ins) FROM up"
        ).format(t=target, c=cols, pk=pk_cols, a=action))
        inserted, updated = cur.fetchone()

    wh.commit()
    src.rollback()
    logging.info("%s: inserted=%s | updated=%s%s", table, inserted, updated,
                 "" if has_updated else " (no updated_at: changes not tracked, only new rows)")


def main():
    for name in ("PROD_PASSWORD", "WAREHOUSE_PASSWORD"):
        if not os.getenv(name):
            raise ValueError(f"Missing {name} in .env")

    src = psycopg2.connect(**SOURCE, connect_timeout=10)
    src.set_session(readonly=True)
    wh = psycopg2.connect(**WAREHOUSE, connect_timeout=10, options="-c lock_timeout=30000")
    try:
        with wh.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(ident(TARGET_SCHEMA)))
        wh.commit()

        for table in TABLES:
            logging.info("Syncing %s ...", table)
            try:
                sync_table(src, wh, table)
            except Exception:
                wh.rollback()
                src.rollback()
                logging.exception("Failed syncing %s", table)
                raise
        logging.info("Production to Bronze sync completed successfully.")
    finally:
        src.close()
        wh.close()


if __name__ == "__main__":
    main()