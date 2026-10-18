import os
import logging
import tempfile
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


def load_table(src, wh, table):
    defs = column_defs(src, SOURCE_SCHEMA, table)
    if not defs:
        raise ValueError(f"Source table {SOURCE_SCHEMA}.{table} not found")

    cols = sql.SQL(", ").join(sql.Identifier(n) for n, _ in defs)
    target = sql.SQL("{}.{}").format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(table))
    ddl = sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(n), sql.SQL(t)) for n, t in defs)

    # Drop + create + load in ONE transaction: if the load fails, the old table stays.
    with wh.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target))
        cur.execute(sql.SQL("CREATE TABLE {} ({})").format(target, ddl))

        with tempfile.TemporaryFile() as buf:  # stream through disk, not memory
            with src.cursor() as s:
                s.copy_expert(
                    sql.SQL("COPY (SELECT {} FROM {}.{}) TO STDOUT").format(
                        cols, sql.Identifier(SOURCE_SCHEMA), sql.Identifier(table)
                    ).as_string(src), buf)
            buf.seek(0)
            cur.copy_expert(
                sql.SQL("COPY {} ({}) FROM STDIN").format(target, cols).as_string(wh), buf)

        cur.execute(sql.SQL("SELECT count(*) FROM {}").format(target))
        rows = cur.fetchone()[0]

    wh.commit()
    src.rollback()
    logging.info("%s: loaded %s rows", table, rows)


def main():
    for name in ("PROD_PASSWORD", "WAREHOUSE_PASSWORD"):
        if not os.getenv(name):
            raise ValueError(f"Missing {name} in .env")

    # lock_timeout: fail with a clear error instead of hanging on a locked table
    src = psycopg2.connect(**SOURCE, connect_timeout=10)
    wh = psycopg2.connect(**WAREHOUSE, connect_timeout=10, options="-c lock_timeout=30000")
    try:
        with wh.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(TARGET_SCHEMA)))
        wh.commit()

        for table in TABLES:
            logging.info("Loading %s ...", table)
            try:
                load_table(src, wh, table)
            except Exception:
                wh.rollback()
                src.rollback()
                logging.exception("Failed loading %s", table)
                raise
        logging.info("Production to Bronze load completed successfully.")
    finally:
        src.close()
        wh.close()


if __name__ == "__main__":
    main()