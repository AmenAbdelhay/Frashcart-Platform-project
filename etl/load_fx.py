"""Load FX rates (GET /v1/rates/history, max 31 days per call) into bronze_fx.fx_rates.

    py load_fx.py 2026-10-16            # from this date until today
    py load_fx.py 2026-10-16 2026-10-20 # from this date until that date
    (or set FX_START_DATE in .env and run without arguments)

Rules:
  * The provider publishes Sun-Thu at 16:00 Cairo, so only those days are expected.
    Fri/Sat, and today before 16:00, are never counted as "missing".
  * Only missing days are requested. Stored days are never overwritten (no duplicates).
  * Every day returned by the API is checked before saving. Bad days are logged, NOT saved.
Exit code: 0 = everything expected is saved, 1 = API failures or rejected data.
"""
import datetime as dt
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
from psycopg2.extras import Json
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

WAREHOUSE = {
    "host": os.getenv("WAREHOUSE_HOST", "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", "5435")),
    "dbname": os.getenv("WAREHOUSE_DB", "postgres"),
    "user": os.getenv("WAREHOUSE_USER", "fresh_wh"),
    "password": os.getenv("WAREHOUSE_PASSWORD"),
}
API_URL = os.getenv("FX_API_URL", "http://localhost:8000").rstrip("/")
API_KEY = os.getenv("FX_API_KEY", "")

CURRENCIES = ("EGP", "SAR", "AED")
BUSINESS_DAYS = (6, 0, 1, 2, 3)   # Sun..Thu  (python: Mon=0 ... Sun=6)
TIMEOUT = 30                      # seconds per API call
RETRIES = 5                       # attempts per API call
PAUSE = 1.0                       # seconds between API calls (rate limit)

try:
    from zoneinfo import ZoneInfo
    CAIRO = ZoneInfo("Africa/Cairo")
except Exception:  # Windows without tzdata
    CAIRO = dt.timezone(dt.timedelta(hours=3))


def fetch(first, last):
    """Ask the API for first..last. Retries with backoff. Returns the list of days."""
    query = urllib.parse.urlencode({"start_date": first, "end_date": last})
    url = f"{API_URL}/v1/rates/history?{query}"
    error = ""

    for attempt in range(1, RETRIES + 1):
        retry_after = 0.0
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json", "X-API-Key": API_KEY})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data = json.load(resp)
            if data.get("base") != "USD" or not isinstance(data.get("days"), list):
                raise ValueError("unexpected answer")
            return data["days"]

        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                sys.exit(f"API refused the request ({e.code}) - check FX_API_KEY in .env")
            if e.code < 500 and e.code not in (408, 429):
                raise RuntimeError(f"{first}..{last}: HTTP {e.code} (not retried)")
            error = f"HTTP {e.code}"
            try:                                   # 429 -> the API tells us how long to wait
                retry_after = min(float(e.headers.get("Retry-After")), 120)
            except (TypeError, ValueError):
                pass
        except Exception as e:                     # timeout, connection error, bad JSON
            error = repr(e)

        if attempt < RETRIES:
            delay = max(retry_after, random.uniform(0.5, 1) * min(60, 2 ** (attempt - 1)))
            logging.warning("%s..%s: %s - retry %s/%s in %.1fs", first, last, error, attempt, RETRIES - 1, delay)
            time.sleep(delay)

    raise RuntimeError(f"{first}..{last}: gave up after {RETRIES} attempts ({error})")


def clean(item, first, last):
    """Return (date, rates) or raise an error if this day must not be trusted."""
    day = dt.date.fromisoformat(item["rate_date"])
    if not first <= day <= last or day.weekday() not in BUSINESS_DAYS:
        raise ValueError(f"{day} is outside the requested period or not a Sun-Thu day")
    for cur in CURRENCIES:
        v = item["rates"][cur]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0:
            raise ValueError(f"{cur} rate is invalid: {v!r}")
    return day, item["rates"]


def stored_days(wh, start, end):
    with wh.cursor() as cur:
        cur.execute("SELECT rate_date FROM bronze_fx.fx_rates WHERE rate_date BETWEEN %s AND %s", (start, end))
        days = {r[0] for r in cur.fetchall()}
    wh.commit()
    return days


def split_windows(days, max_days=31):
    """Group sorted days into [first, last] windows of at most max_days (one API call each)."""
    windows = []
    for d in days:
        if windows and (d - windows[-1][0]).days < max_days:
            windows[-1][1] = d
        else:
            windows.append([d, d])
    return windows


def main():
    if not WAREHOUSE["password"]:
        raise ValueError("Missing WAREHOUSE_PASSWORD in .env")

    # ---- dates
    args = sys.argv[1:]
    start_text = args[0] if args else os.getenv("FX_START_DATE")
    if not start_text or len(args) > 2:
        sys.exit(__doc__)
    now = dt.datetime.now(CAIRO)
    start = dt.date.fromisoformat(start_text)                       # format: 2026-10-16
    end = dt.date.fromisoformat(args[1]) if len(args) == 2 else now.date()

    # today is only expected after 16:00 Cairo, otherwise the latest expected day is yesterday
    latest_published = now.date() if now.hour >= 16 else now.date() - dt.timedelta(days=1)
    latest = min(end, latest_published)
    expected = [start + dt.timedelta(n) for n in range((latest - start).days + 1)
                if (start + dt.timedelta(n)).weekday() in BUSINESS_DAYS]

    wh = psycopg2.connect(**WAREHOUSE, connect_timeout=10)
    failed, rejected = set(), {}
    try:
        with wh.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze_fx")
            cur.execute("""CREATE TABLE IF NOT EXISTS bronze_fx.fx_rates (
                rate_date DATE PRIMARY KEY, base_currency TEXT NOT NULL, rates JSONB NOT NULL,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
        wh.commit()

        # ---- only the missing days
        stored = stored_days(wh, start, end)
        todo = [d for d in expected if d not in stored]
        windows = split_windows(todo)
        logging.info("%s expected day(s), %s missing -> %s API call(s)", len(expected), len(todo), len(windows))

        errors_in_row = 0
        for first, last in windows:
            days_here = {d for d in todo if first <= d <= last}
            if errors_in_row >= 3:                                  # API looks down: stop
                failed |= days_here
                continue
            try:
                items = fetch(first, last)
                errors_in_row = 0
            except RuntimeError as e:
                logging.error("%s", e)
                failed |= days_here
                errors_in_row += 1
                continue

            # ---- validate every day
            good, conflicts = {}, set()
            for item in items:
                try:
                    day, rates = clean(item, first, last)
                except (KeyError, TypeError, ValueError) as e:
                    key = str(item.get("rate_date")) if isinstance(item, dict) else "?"
                    rejected[key] = str(e)
                    logging.error("REJECTED %s: %s", key, e)
                    continue
                if day in good and good[day] != rates:
                    conflicts.add(day)
                good[day] = rates
            for day in conflicts:
                good.pop(day)
                rejected[str(day)] = "listed twice with different rates"
                logging.error("REJECTED %s: listed twice with different rates", day)

            # ---- save the window (existing days are never overwritten)
            with wh.cursor() as cur:
                for day, rates in good.items():
                    cur.execute("INSERT INTO bronze_fx.fx_rates (rate_date, base_currency, rates) "
                                "VALUES (%s, 'USD', %s) ON CONFLICT (rate_date) DO NOTHING",
                                (day, Json(rates)))
            wh.commit()
            logging.info("Window %s..%s: %s day(s) saved", first, last, len(good))
            time.sleep(PAUSE)

        in_db = stored_days(wh, start, end)                         # final check on the real table
    finally:
        wh.close()

    # ---- summary
    missing = [d for d in expected if d not in in_db]
    not_returned = [d for d in missing if d not in failed and str(d) not in rejected]
    logging.info("Summary: %s expected, %s in warehouse, %s missing",
                 len(expected), len(expected) - len(missing), len(missing))
    if not_returned:
        logging.warning("Days the API did not return (will retry next run): %s",
                        ", ".join(map(str, not_returned[:20])))
    if failed or rejected:
        logging.error("NOT loaded: %s", ", ".join(sorted(map(str, failed | set(rejected)))[:20]))
        sys.exit(1)


if __name__ == "__main__":
    main()