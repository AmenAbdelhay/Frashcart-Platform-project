"""Load supplier stock files (source_files/suppliers/<supplier>/*.csv) into bronze_suppliers.

  fresh/fresh_stock_YYYY-MM-DD.csv     -> bronze_suppliers.fresh
  general/GENERAL_STOCK_YYYYMMDD.csv   -> bronze_suppliers.general

The two suppliers use different formats, so each has its own table; columns are taken
from each file's own header and stored as TEXT exactly as received (raw bronze).

Rule: load each file once; late files must still be loaded.
We never filter by date / modified time / "latest file". A ledger table
(bronze_suppliers.etl_loaded_files) records every file loaded; each run loads every file
on disk that is NOT in the ledger - so a GENERAL file that shows up the next morning
(or any day later) is picked up on the next run, and nothing is ever loaded twice.
Rows + ledger entry are committed in one transaction (no half-loaded files).

Extra columns on every row: _source_file, _file_hash, _stock_date (parsed from the
file name = the day the stock snapshot is for), _loaded_at.
"""
import csv
import datetime
import hashlib
import io
import logging
import os
import re
import sys
import tempfile
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

WAREHOUSE = {
    "host": os.getenv("WAREHOUSE_HOST", "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", "5435")),
    "dbname": os.getenv("WAREHOUSE_DB", "postgres"),
    "user": os.getenv("WAREHOUSE_USER", "fresh_wh"),
    "password": os.getenv("WAREHOUSE_PASSWORD"),
}

SUPPLIERS_DIR = Path(os.getenv("SUPPLIERS_DIR", ROOT / "source_files" / "suppliers"))
SCHEMA = "bronze_suppliers"
LEDGER = "etl_loaded_files"
META = ["_source_file", "_file_hash", "_stock_date"]


def stock_date(name):
    """2026-10-17 (fresh) or 20261017 (general) in the file name -> date, else None."""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})|(\d{4})(\d{2})(\d{2})", name)
    if not m:
        return None
    y, mo, d = [int(g) for g in m.groups() if g]
    try:
        return datetime.date(y, mo, d)
    except ValueError:
        return None


def read_csv(path):
    """-> (columns, rows). Detects encoding and delimiter; every value stays a string."""
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")

    first = text.split("\n", 1)[0]
    delimiter = max([",", ";", "\t", "|"], key=first.count)

    data = [r for r in csv.reader(io.StringIO(text, newline=""), delimiter=delimiter) if r]
    if not data:
        return [], []
    header, rows = data[0], data[1:]
    width = max([len(header)] + [len(r) for r in rows])
    header = [(h.strip() or f"col_{i + 1}") for i, h in enumerate(header)] + \
             [f"col_{i + 1}" for i in range(len(header), width)]
    seen = {}
    for i, h in enumerate(header):  # repeated header names -> name_2, name_3 ...
        seen[h] = seen.get(h, 0) + 1
        if seen[h] > 1:
            header[i] = f"{h}_{seen[h]}"
    return header, [r + [None] * (width - len(r)) for r in rows]


def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def table_name(folder):
    return re.sub(r"\W+", "_", folder.lower()).strip("_") or "files"


def load_file(wh, table, rel_path, path):
    cols, rows = read_csv(path)
    digest, day = file_hash(path), stock_date(path.name)
    if day is None:
        logging.warning("%s: no date found in file name - _stock_date will be NULL", rel_path)
    target = sql.SQL("{}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(table))

    with wh.cursor() as cur:
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                _source_file TEXT, _file_hash TEXT, _stock_date DATE,
                _loaded_at TIMESTAMPTZ DEFAULT now())
        """).format(target))
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s", (SCHEMA, table))
        existing = {r[0] for r in cur.fetchall()}
        for c in cols:  # a later file with extra columns -> columns are added, never dropped
            if c not in existing:
                cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(target, sql.Identifier(c)))

        if rows:
            with tempfile.TemporaryFile("w+", newline="", encoding="utf-8") as buf:
                w = csv.writer(buf)
                for r in rows:
                    w.writerow([rel_path, digest, day] + r)
                buf.seek(0)
                cur.copy_expert(
                    sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
                        target, sql.SQL(", ").join(map(sql.Identifier, META + cols))
                    ).as_string(wh), buf)

        cur.execute(sql.SQL("INSERT INTO {}.{} (file_path, table_name, file_hash, row_count) "
                            "VALUES (%s, %s, %s, %s)").format(sql.Identifier(SCHEMA), sql.Identifier(LEDGER)),
                    (rel_path, table, digest, len(rows)))
    wh.commit()  # rows + ledger entry together
    return len(rows)


def main():
    if not WAREHOUSE["password"]:
        raise ValueError("Missing WAREHOUSE_PASSWORD in .env")
    if not SUPPLIERS_DIR.is_dir():
        raise FileNotFoundError(f"Suppliers folder not found: {SUPPLIERS_DIR}")

    wh = psycopg2.connect(**WAREHOUSE, connect_timeout=10)
    errors = new_files = 0
    try:
        with wh.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtext('load_suppliers'))")  # no parallel runs
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    file_path TEXT PRIMARY KEY, table_name TEXT, file_hash TEXT,
                    row_count BIGINT, loaded_at TIMESTAMPTZ DEFAULT now())
            """).format(sql.Identifier(SCHEMA), sql.Identifier(LEDGER)))
            cur.execute(sql.SQL("SELECT file_path, file_hash FROM {}.{}").format(
                sql.Identifier(SCHEMA), sql.Identifier(LEDGER)))
            loaded = dict(cur.fetchall())
        wh.commit()

        for folder in sorted(p for p in SUPPLIERS_DIR.iterdir() if p.is_dir()):
            files = sorted(p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() == ".csv")
            for path in files:
                rel = path.relative_to(SUPPLIERS_DIR).as_posix()
                if rel in loaded:
                    if file_hash(path) != loaded[rel]:
                        logging.warning("%s changed after it was loaded - NOT reloaded (load-once rule)", rel)
                    continue
                try:
                    n = load_file(wh, table_name(folder.name), rel, path)
                    new_files += 1
                    logging.info("Loaded %s (%s rows) -> %s.%s", rel, n, SCHEMA, table_name(folder.name))
                except Exception:
                    wh.rollback()
                    errors += 1
                    logging.exception("Failed loading %s (will retry next run)", rel)

        logging.info("Done. %s new file(s) loaded, %s failed.", new_files, errors)
    finally:
        wh.close()

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()