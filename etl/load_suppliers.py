"""
Load supplier CSV files (source_files/suppliers/<supplier>/*.csv) into bronze_suppliers.

  fresh/*.csv   -> bronze_suppliers.fresh
  general/*.csv -> bronze_suppliers.general

How it works (simple):
  - Every file is tracked in a ledger table (file_path + file_hash).
  - NEW file      -> load it.
  - CHANGED file  -> (same name, different content) delete its old rows, load the new ones.
  - SAME file     -> skip.
  So late files are still picked up, and nothing is ever duplicated.
  Rows + ledger entry are saved in ONE transaction (no half-loaded files).

All values are stored as TEXT exactly as received, plus 4 extra columns:
  _source_file, _file_hash, _stock_date (date taken from the file name), _loaded_at
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
META_COLS = ["_source_file", "_file_hash", "_stock_date"]


# ---------------------------------------------------------------- helpers
def stock_date(name):
    """Find 2026-10-17 or 20261017 in the file name. Returns a date or None."""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})|(\d{4})(\d{2})(\d{2})", name)
    if not m:
        return None
    y, mo, d = [int(g) for g in m.groups() if g]
    try:
        return datetime.date(y, mo, d)
    except ValueError:
        return None


def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path):
    """Return (columns, rows). Detects encoding + delimiter. Values stay strings."""
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("cp1252")

    first_line = text.split("\n", 1)[0]
    delimiter = max([",", ";", "\t", "|"], key=first_line.count)

    data = [r for r in csv.reader(io.StringIO(text, newline=""), delimiter=delimiter) if r]
    if not data:
        return [], []

    header, rows = data[0], data[1:]
    width = max([len(header)] + [len(r) for r in rows])

    # clean header: empty -> col_N, repeated -> name_2, name_3 ...
    cols = [(h.strip() or f"col_{i + 1}") for i, h in enumerate(header)]
    cols += [f"col_{i + 1}" for i in range(len(cols), width)]
    seen = {}
    for i, c in enumerate(cols):
        seen[c] = seen.get(c, 0) + 1
        if seen[c] > 1:
            cols[i] = f"{c}_{seen[c]}"

    rows = [r + [None] * (width - len(r)) for r in rows]  # pad short rows
    return cols, rows


# ---------------------------------------------------------------- load one file
def load_file(wh, table, rel_path, path):
    cols, rows = read_csv(path)
    digest = file_hash(path)
    day = stock_date(path.name)
    if day is None:
        logging.warning("%s: no date in file name - _stock_date will be NULL", rel_path)

    target = sql.SQL("{}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(table))
    ledger = sql.SQL("{}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(LEDGER))

    with wh.cursor() as cur:
        # 1) make sure the table exists and has every column of this file
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                _source_file TEXT, _file_hash TEXT, _stock_date DATE,
                _loaded_at TIMESTAMPTZ DEFAULT now())
        """).format(target))
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s", (SCHEMA, table))
        existing = {r[0] for r in cur.fetchall()}
        for c in cols:
            if c not in existing:
                cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(target, sql.Identifier(c)))

        # 2) remove the old version of this file (nothing to remove for a new file)
        cur.execute(sql.SQL("DELETE FROM {} WHERE _source_file = %s").format(target), (rel_path,))

        # 3) insert the rows with COPY (fast)
        if rows:
            with tempfile.TemporaryFile("w+", newline="", encoding="utf-8") as buf:
                writer = csv.writer(buf)
                for r in rows:
                    writer.writerow([rel_path, digest, day] + r)
                buf.seek(0)
                copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
                    target, sql.SQL(", ").join(map(sql.Identifier, META_COLS + cols)))
                cur.copy_expert(copy_sql.as_string(wh), buf)

        # 4) record the file in the ledger (insert, or update if the file changed)
        cur.execute(sql.SQL("""
            INSERT INTO {} (file_path, table_name, file_hash, row_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (file_path) DO UPDATE
               SET file_hash = EXCLUDED.file_hash,
                   row_count = EXCLUDED.row_count,
                   loaded_at = now()
        """).format(ledger), (rel_path, table, digest, len(rows)))

    wh.commit()  # rows + ledger together
    return len(rows)


# ---------------------------------------------------------------- main
def main():
    if not WAREHOUSE["password"]:
        raise ValueError("Missing WAREHOUSE_PASSWORD in .env")
    if not SUPPLIERS_DIR.is_dir():
        raise FileNotFoundError(f"Suppliers folder not found: {SUPPLIERS_DIR}")

    wh = psycopg2.connect(**WAREHOUSE, connect_timeout=10)
    new_count = changed_count = errors = 0

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
            loaded = dict(cur.fetchall())  # {file_path: hash}
        wh.commit()

        for folder in sorted(p for p in SUPPLIERS_DIR.iterdir() if p.is_dir()):
            table = re.sub(r"\W+", "_", folder.name.lower()).strip("_") or "files"

            for path in sorted(p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() == ".csv"):
                rel = path.relative_to(SUPPLIERS_DIR).as_posix()

                if rel not in loaded:
                    status = "new"
                elif file_hash(path) != loaded[rel]:
                    status = "changed"
                else:
                    continue  # already loaded, same content

                try:
                    n = load_file(wh, table, rel, path)
                    if status == "new":
                        new_count += 1
                    else:
                        changed_count += 1
                    logging.info("%s file %s: %s rows -> %s.%s", status.upper(), rel, n, SCHEMA, table)
                except Exception:
                    wh.rollback()
                    errors += 1
                    logging.exception("Failed loading %s (will retry next run)", rel)

        logging.info("Done. new=%s | changed=%s | failed=%s", new_count, changed_count, errors)
    finally:
        wh.close()

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()