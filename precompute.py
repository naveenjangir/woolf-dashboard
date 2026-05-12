#!/usr/bin/env python3
"""
Woolf Dashboard — cache pre-warmer (Option B: Railway Cron Service).

Runs all expensive BigQuery queries for the current month and writes results
to the persistent volume cache.  Because the web service reads from the same
volume, every page load after this script runs will hit disk instead of BigQuery.

SCHEDULE
  Run via a Railway Cron Service on the same project, mounted to the same
  Persistent Volume as the web service.
  Recommended: every 2 hours  →  cron expression "0 */2 * * *"

ENVIRONMENT VARIABLES (same as web service)
  WOOLF_CACHE_DIR   Path to the volume mount, e.g. /data/woolf_cache
  MB_HOST / MB_SESSION_TOKEN / MB_DATABASE_ID  (same Metabase creds as web)

USAGE
  python precompute.py                  # current month, skip already-fresh entries
  python precompute.py 2026 4           # specific month
  python precompute.py --force          # force-refresh all entries regardless of TTL
  python precompute.py 2026 4 --force
"""

import sys
import time
import calendar
import argparse
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Import query internals from the shared module ─────────────────────────────
# All _query functions are the raw BigQuery functions (no caching wrapper).
# We call them here, then write results to the shared cache via _cache_set.
from queries import (
    _CACHE_DIR,
    _cache_set,
    _cache_get,
    # Raw query functions (no _dcache wrapper)
    _get_colleges_query,
    _get_pricing_config_query,
    _historical_activity_counts_query,
    _seat_exp_revenue_query,
    _st_new_counts_query,
    _st_conv_pba_counts_query,
    _revsh_exp_revenue_query,
    _revsh_archived_30d_query,
    _rpl_counts_query,
    _rpl_admissions_query,
    _st_wlh_by_college_query,
    _completion_rates_seasonal_query,
    _oxford_sbs_query,
    # Date helpers
    prev_month,
    year_ago,
    month_bounds,
)


# ── Helper ─────────────────────────────────────────────────────────────────────

def _run_and_cache(key: str, fn, *args):
    """Execute fn(*args), persist result to disk cache, return timing."""
    t = time.time()
    result = fn(*args)
    _cache_set(key, result)
    elapsed = time.time() - t
    size = len(result) if hasattr(result, "__len__") else 1
    return key, elapsed, size


# ── Main pre-computation routine ──────────────────────────────────────────────

def precompute(year: int, month: int, force: bool = False) -> None:
    today     = date.today()
    is_current = (year == today.year and month == today.month)
    today_day  = today.day if is_current else calendar.monthrange(year, month)[1]

    py,  pm  = prev_month(year, month)
    yy1, my1 = year_ago(year, month)

    print(f"\n{'='*64}")
    print(f"  Woolf cache pre-warmer  |  {year}-{month:02d}  |  force={force}")
    print(f"  Cache dir : {_CACHE_DIR}")
    print(f"  Today_day : {today_day}")
    print(f"{'='*64}")

    # ── Task registry: key → (ttl_hours, fn, positional_args) ─────────────────
    # Key strings MUST match exactly what the _dcache wrappers in queries.py use.
    tasks: dict[str, tuple] = {

        # ─ Long-lived: colleges list & fee config (weekly)
        "get_colleges":                           (168.0, _get_colleges_query,            ()),
        "get_pricing_config":                     (168.0, _get_pricing_config_query,      ()),

        # ─ Monthly-frozen: historical M-1 / Y-1 activity counts
        f"hist_acts_{py}_{pm}_{yy1}_{my1}_{today_day}":
                                                  (720.0, _historical_activity_counts_query,
                                                   (py, pm, yy1, my1, today_day)),

        # ─ Monthly-frozen: ST >25h WLH (only changes as new ST students cross 25h)
        f"_st_wlh_by_college_{year}_{month}":    (720.0, _st_wlh_by_college_query,       (year, month)),

        # ─ Daily: everything that changes with new enrolments day-to-day
        f"completion_rates_{year}_{month}_{today_day}":
                                                  (24.0,  _completion_rates_seasonal_query,
                                                   (year, month, today_day)),
        f"_st_new_counts_{year}_{month}":         (24.0,  _st_new_counts_query,           (year, month)),
        f"_st_conv_pba_counts_{year}_{month}":    (24.0,  _st_conv_pba_counts_query,      (year, month)),
        f"_rpl_counts_{year}_{month}":            (24.0,  _rpl_counts_query,              (year, month)),
        f"_oxford_sbs_{year}_{month}":            (24.0,  _oxford_sbs_query,              (year, month)),
        f"_revsh_archived_30d_{year}_{month}":    (24.0,  _revsh_archived_30d_query,      (year, month)),
        f"_seat_exp_revenue_{year}_{month}":      (24.0,  _seat_exp_revenue_query,        (year, month)),
        f"_revsh_exp_revenue_{year}_{month}":     (24.0,  _revsh_exp_revenue_query,       (year, month)),
        f"_rpl_admissions_{year}_{month}":        (24.0,  _rpl_admissions_query,          (year, month)),
    }

    # ── Decide which tasks need running (skip fresh cache entries unless --force)
    to_run: dict[str, tuple] = {}
    for key, (ttl, fn, args) in tasks.items():
        if force or _cache_get(key, ttl) is None:
            to_run[key] = (fn, args)
        else:
            f = _CACHE_DIR / f"{key}.pkl"
            age_h = (time.time() - f.stat().st_mtime) / 3600
            print(f"  skip  {key:<58s}  (age {age_h:.1f}h < {ttl:.0f}h TTL)")

    if not to_run:
        print("\n  All entries are fresh — nothing to do.\n")
        return

    # ── Run stale/missing tasks in parallel ───────────────────────────────────
    print(f"\n  Running {len(to_run)} queries in parallel ...\n")
    t_total = time.time()
    errors: list[tuple[str, Exception]] = []

    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {
            pool.submit(_run_and_cache, key, fn, *args): key
            for key, (fn, args) in to_run.items()
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                _, elapsed, size = future.result()
                print(f"  ✓  {key:<58s}  {elapsed:5.1f}s  ({size} rows)")
            except Exception as exc:
                errors.append((key, exc))
                print(f"  ✗  {key:<58s}  ERROR: {exc}")

    elapsed_total = time.time() - t_total
    print(f"\n  Done in {elapsed_total:.1f}s  |  "
          f"{len(to_run) - len(errors)} succeeded  |  {len(errors)} failed")

    if errors:
        print("\n  Failed queries (will be retried on next run):")
        for key, exc in errors:
            print(f"    {key}: {exc}")
        sys.exit(1)   # non-zero exit so Railway marks the cron run as failed


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pre-warm the Woolf dashboard query cache.")
    parser.add_argument("year",  nargs="?", type=int, default=date.today().year,
                        help="Year to pre-compute (default: current year)")
    parser.add_argument("month", nargs="?", type=int, default=date.today().month,
                        help="Month to pre-compute (default: current month)")
    parser.add_argument("--force", action="store_true",
                        help="Force-refresh all cache entries regardless of TTL")
    a = parser.parse_args()

    precompute(a.year, a.month, force=a.force)
