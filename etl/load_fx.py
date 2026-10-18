"""Load FX rates (GET /v1/rates/history, 31 days max per call) into bronze_fx.fx_rates.

    python etl/load_fx.py 2024-10-01 [end]      # end defaults to today; or set FX_START_DATE in .env

Provider behaviour handled here:
  * Published Sun-Thu at 16:00 Cairo  -> only those days are expected; Fri/Sat and today-before-16:00 are never "missing".
  * Rate limit per minute            -> /history (few calls), pause between calls, honour Retry-After on 429, slow down.
  * Not 100% reliable                -> timeout + retry with exponential backoff + jitter, stop after 3 failed windows in a
                                        row, and every answer is checked: a day is stored only if it is inside the requested
                                        period, is Sun-Thu, and EGP/SAR/AED are numbers > 0 (and not duplicated with different
                                        values). Bad days are logged, NOT stored, and the run exits 1.
  * Re-runs are safe: only missing days are requested; stored days are never overwritten.
Exit 0 = every expected day is stored (days the API did not return are listed as warnings); 1 = failures/rejected data.
"""
import datetime as dt
import http.client
import json
import logging
import math
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
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
API_URL = os.getenv("FX_API_URL", "http://localhost:8000").rstrip("/")  # inside a container: http://fx-api:8000
API_KEY = os.getenv("FX_API_KEY")
TIMEOUT, RETRIES, BACKOFF, GAP = 30, 5, 1.0, 1.0   # seconds / attempts / first backoff / pause between calls
CURRENCIES = ("EGP", "SAR", "AED")
BUSINESS_DAYS = (6, 0, 1, 2, 3)                    # Sun..Thu (python: Mon=0 .. Sun=6)
TABLE = sql.SQL("bronze_fx.fx_rates")

try:
    from zoneinfo import ZoneInfo
    CAIRO = ZoneInfo("Africa/Cairo")
except Exception:  # Windows without tzdata (pip install tzdata)
    CAIRO = dt.timezone(dt.timedelta(hours=3))

_last_call = 0.0


def now_cairo():
    return dt.datetime.now(CAIRO)


def fetch(first, last):
    """History for first..last -> list of day entries. Raises RuntimeError when the API cannot give a valid answer."""
    global _last_call, GAP
    url = f"{API_URL}/v1/rates/history?" + urllib.parse.urlencode({"start_date": first, "end_date": last})
    label, err = f"history {first}..{last}", None
    for attempt in range(1, RETRIES + 1):
        time.sleep(max(0, _last_call + GAP - time.monotonic()))   # stay under the per-minute limit
        _last_call = time.monotonic()
        wait = 0.0
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json", "X-API-Key": API_KEY or ""})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                d = json.load(r)
            if not (isinstance(d, dict) and isinstance(d.get("days"), list) and d.get("base") == "USD"
                    and (d.get("start_date"), d.get("end_date")) == (str(first), str(last))):
                raise ValueError("unexpected answer")
            return d["days"]
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                sys.exit(f"API refused the request ({e.code}) - check FX_API_KEY in .env")
            if e.code < 500 and e.code not in (408, 429):
                raise RuntimeError(f"{label}: HTTP {e.code} (not retried)")
            err = f"HTTP {e.code}"
            if e.code == 429:
                GAP = min(GAP * 2, 10)                      # we are too fast -> slow down
            try:
                wait = min(float(e.headers.get("Retry-After")), 120)
            except (TypeError, ValueError):
                pass
        except (OSError, http.client.HTTPException, ValueError) as e:   # timeout, connection error, bad JSON/answer
            err = repr(e)
        if attempt < RETRIES:
            delay = max(wait, random.uniform(0.5, 1) * min(60, BACKOFF * 2 ** (attempt - 1)))
            logging.warning("%s: %s - retry %s/%s in %.1fs", label, err, attempt, RETRIES - 1, delay)
            time.sleep(delay)
    raise RuntimeError(f"{label}: gave up after {RETRIES} attempts ({err})")


def clean(item, lo, hi):
    """-> (date, rates) or raises ValueError/KeyError/TypeError for a day we must not trust."""
    d, rates = dt.date.fromisoformat(item["rate_date"]), item["rates"]
    if not lo <= d <= hi or d.weekday() not in BUSINESS_DAYS:
        raise ValueError(f"{d} is outside the requested period or not a Sun-Thu day")
    for c in CURRENCIES:
        v = rates[c]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0:
            raise ValueError(f"{c} rate is invalid: {v!r}")
    return d, rates


def main():
    if not WAREHOUSE["password"]:
        raise ValueError("Missing WAREHOUSE_PASSWORD in .env")
    if not API_KEY:
        logging.warning("FX_API_KEY is not set - the API will answer 401")
    args = sys.argv[1:]
    start_s = args[0] if args else os.getenv("FX_START_DATE")
    if not start_s or len(args) > 2:
        sys.exit(__doc__)
    now = now_cairo()
    start = dt.date.fromisoformat(start_s)
    end = dt.date.fromisoformat(args[1]) if len(args) == 2 else now.date()

    # expected = business days up to the latest day that can be published (today only after 16:00 Cairo)
    latest = min(end, now.date() if now.hour >= 16 else now.date() - dt.timedelta(days=1))
    expected = [start + dt.timedelta(n) for n in range((latest - start).days + 1)
                if (start + dt.timedelta(n)).weekday() in BUSINESS_DAYS]

    wh = psycopg2.connect(**WAREHOUSE, connect_timeout=10)
    failed, rejected = set(), {}
    try:
        with wh.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze_fx")
            cur.execute(sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
                rate_date DATE PRIMARY KEY, base_currency TEXT NOT NULL, rates JSONB NOT NULL,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT now())""").format(TABLE))
            q = sql.SQL("SELECT rate_date FROM {} WHERE rate_date BETWEEN %s AND %s").format(TABLE)
            cur.execute(q, (start, end))
            stored = {r[0] for r in cur.fetchall()}
        wh.commit()

        todo = [d for d in expected if d not in stored]
        windows = []                                       # group missing days into <=31-day calls
        for d in todo:
            if windows and (d - windows[-1][0]).days < 31:
                windows[-1][1] = d
            else:
                windows.append([d, d])
        logging.info("%s expected business day(s), %s missing -> %s API call(s)", len(expected), len(todo), len(windows))

        fails = 0
        for first, last in windows:
            days_here = {d for d in todo if first <= d <= last}
            if fails >= 3:                                 # API looks down: stop hammering it
                failed |= days_here
                continue
            try:
                items = fetch(first, last)
                fails = 0
            except RuntimeError as e:
                logging.error("%s", e)
                failed |= days_here
                fails += 1
                continue

            good, dup = {}, set()
            for it in items:
                try:
                    d, rates = clean(it, first, last)
                except (KeyError, TypeError, ValueError) as e:
                    key = str(it.get("rate_date")) if isinstance(it, dict) else "?"
                    rejected[key] = str(e)
                    logging.error("REJECTED %s: %s", key, e)
                    continue
                if d in dup:
                    continue
                if good.get(d, rates) != rates:
                    dup.add(d)
                    good.pop(d)
                    rejected[str(d)] = "listed twice with different rates"
                    logging.error("REJECTED %s: listed twice with different rates", d)
                    continue
                good[d] = rates

            with wh.cursor() as cur:                       # the whole window is stored, or nothing
                for d, rates in good.items():
                    cur.execute(sql.SQL("INSERT INTO {} (rate_date, base_currency, rates) VALUES (%s, 'USD', %s) "
                                        "ON CONFLICT (rate_date) DO NOTHING").format(TABLE), (d, Json(rates)))
            wh.commit()
            logging.info("Window %s..%s: %s day(s) stored", first, last, len(good))

        # final check against what is REALLY in the warehouse, not against our own counters
        with wh.cursor() as cur:
            cur.execute(q, (start, end))
            in_db = {r[0] for r in cur.fetchall()}
        wh.commit()
    finally:
        wh.close()

    missing = [d for d in expected if d not in in_db]
    unreturned = [d for d in missing if d not in failed and str(d) not in rejected]
    logging.info("Summary: %s expected, %s in warehouse, %s missing", len(expected), len(expected) - len(missing), len(missing))
    if unreturned:
        logging.warning("Business days the API did not return (retried next run): %s", ", ".join(map(str, unreturned[:20])))
    if failed or rejected:
        logging.error("NOT loaded - API failed or data rejected: %s", ", ".join(sorted(map(str, failed | set(rejected)))[:20]))
        sys.exit(1)


if __name__ == "__main__":
    main()