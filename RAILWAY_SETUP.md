# Railway Persistent Cache Setup

This doc explains how to configure a Railway Persistent Volume + Cron Service
so the dashboard always loads instantly, even right after a redeploy.

## How it works

```
  Railway Persistent Volume  (/data/woolf_cache)
         │                              │
  ┌──────┴──────┐              ┌────────┴────────┐
  │  Web Service │              │  Cron Service   │
  │  (app.py)   │ reads cache  │ (precompute.py) │
  │             │◄─────────────│                 │
  └─────────────┘              └─────────────────┘
                                  runs every 2h
```

- Web service reads from volume → instant loads (no BigQuery)  
- Cron service writes to volume → always-fresh data  
- Both share the same volume, so writes are immediately visible to the web service

---

## Step 1 — Create a Persistent Volume

1. Open your Railway project dashboard
2. **New** → **Volume**
3. Name it `woolf-cache`
4. Size: **1 GB** (pickle files are tiny; 1 GB is plenty for years of cache)
5. Click **Create**

---

## Step 2 — Mount the Volume to the Web Service

1. Click your **web service** (the one running `app.py`)
2. Go to **Settings → Mounts**
3. Click **+ Add Volume Mount**
4. Volume: `woolf-cache`
5. Mount path: `/data/woolf_cache`
6. **Save** (triggers a redeploy)

---

## Step 3 — Add the `WOOLF_CACHE_DIR` Environment Variable to the Web Service

1. Still on the web service, go to **Variables**
2. Add:
   ```
   WOOLF_CACHE_DIR = /data/woolf_cache
   ```
3. **Save** (triggers another redeploy — after this the web service writes to the volume)

---

## Step 4 — Create the Cron Service

1. In your Railway project, **New** → **Empty Service**
2. Name it `woolf-cache-warmer`
3. Go to **Settings** → **Source** → connect to the same GitHub repo + branch as the web service
4. Set the **Start Command**:
   ```
   python precompute.py
   ```
5. Go to **Settings → Cron** → enable cron and set schedule:
   ```
   0 */2 * * *
   ```
   *(runs every 2 hours; adjust to `0 * * * *` for every hour if you prefer)*

---

## Step 5 — Mount the Volume to the Cron Service

1. On the **cron service**, go to **Settings → Mounts**
2. Add the same volume:
   - Volume: `woolf-cache`
   - Mount path: `/data/woolf_cache`
3. **Save**

---

## Step 6 — Add Environment Variables to the Cron Service

Copy all environment variables from the web service to the cron service:

```
WOOLF_CACHE_DIR    = /data/woolf_cache
MB_HOST            = <same as web service>
MB_SESSION_TOKEN   = <same as web service>
MB_DATABASE_ID     = <same as web service>
```

---

## Step 7 — First Run (Manual)

Trigger a manual run of the cron service to pre-warm the cache immediately:

1. On the cron service → **Settings → Cron** → **Trigger Run**
2. Watch the logs — should see ~13 queries run in parallel, finish in ~60s

After this the web service will load in ~3–5s instead of 30–60s.

---

## Cache TTL reference

| Cache entry                   | TTL     | Refreshed by cron?          |
|-------------------------------|---------|-----------------------------|
| `get_colleges`                | 168h    | Only when stale (once/week) |
| `get_pricing_config`          | 168h    | Only when stale (once/week) |
| `hist_acts_*` (M-1 / Y-1)    | 720h    | Only when stale (once/month)|
| `_st_wlh_by_college_*`        | 720h    | Only when stale (once/month)|
| `completion_rates_*`          | 24h     | Every 2h cron run           |
| `_st_new_counts_*`            | 24h     | Every 2h cron run           |
| `_st_conv_pba_counts_*`       | 24h     | Every 2h cron run           |
| `_rpl_counts_*`               | 24h     | Every 2h cron run           |
| `_oxford_sbs_*`               | 24h     | Every 2h cron run           |
| `_revsh_archived_30d_*`       | 24h     | Every 2h cron run           |
| `_seat_exp_revenue_*`         | 24h     | Every 2h cron run           |
| `_revsh_exp_revenue_*`        | 24h     | Every 2h cron run           |
| `_rpl_admissions_*`           | 24h     | Every 2h cron run           |

---

## What still runs live on every page load

Even with the cache warm, two queries always run against BigQuery:

| Query               | Why live?                           | Typical time |
|---------------------|-------------------------------------|--------------|
| `_current_activity_counts` | This-month enrolments change throughout the day | ~2s |
| `_active_base`      | Live active student count           | ~3s          |

So worst-case page load after this setup = **~5s** (down from 30–60s).

---

## Force-refresh (manual)

To immediately refresh all cache entries (e.g. after a data fix):

```bash
# In a Railway shell on the cron service, or locally:
python precompute.py --force
```

---

## Local development

No changes needed locally. When `WOOLF_CACHE_DIR` is not set, the code
falls back to `.query_cache/` inside the repo directory (already in `.gitignore`).
