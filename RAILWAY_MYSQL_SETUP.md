# Railway MySQL Setup Guide

## Step 1 — Add MySQL to your Railway project

1. Go to **railway.app** → your project
2. Click **+ New** → **Database** → **MySQL**
3. Railway creates the MySQL service and auto-sets environment variables

## Step 2 — Connect your app to the database

Railway automatically injects these variables into your app:
- `MYSQLHOST`
- `MYSQLPORT`
- `MYSQLUSER`
- `MYSQLPASSWORD`
- `MYSQLDATABASE`

**No manual config needed** — the app reads these automatically.

Alternatively, go to your MySQL service → **Connect** tab → copy the
`DATABASE_URL` and add it as an environment variable in your app service.

## Step 3 — Deploy

Push or redeploy your app. On first boot:
1. App connects to MySQL
2. Creates all tables automatically (schema auto-migration)
3. Seeds the 1,000 transactions + 690 alerts + 286 cases
4. Dashboard loads with real data in seconds

## Step 4 — Verify

Check Railway logs — you should see:
```
[DB] Connected to MySQL at ...
[DB] Schema ready
[DataStore] Seeding database from dataset...  (first run only)
[DataStore] Database seeded: 690 alerts, 286 cases
  690 alerts loaded
  READY
```

On subsequent deployments:
```
[DB] Connected to MySQL at ...
[DataStore] Loaded from DB: 690 alerts, 286 cases
  690 alerts loaded
  READY
```

## Local Development (no database)

Without `DATABASE_URL` or `MYSQLHOST` set, the app automatically
falls back to loading from `transactions_cache.json` — no setup needed.

## Tables Created

| Table | Purpose |
|---|---|
| `transactions` | All 81-column transaction records |
| `alerts` | 690 ML-flagged alerts with scores |
| `cases` | 286 investigation cases |
| `sar_records` | SAR filings |
| `audit_log` | All system actions |
| `users` | Analyst / supervisor accounts |
| `notifications` | System alerts |
| `app_config` | Runtime configuration |

## Default Users (seeded on first run)

| Username | Password | Role |
|---|---|---|
| admin | admin123 | Admin |
| ransford | pass123 | Supervisor |
| jmensah | pass123 | Analyst |
| aowusu | pass123 | Analyst |
