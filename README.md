# ADWISER backend

FastAPI provides the 24 authenticated Monitor operations and three read-only Signal operations used by the existing dashboard. PostgreSQL owns identity, inventory, permissions, alerts, events and sampled schedule metadata; InfluxDB 3 Core owns raw telemetry. Background workers reconcile inventory, evaluate alert rules and copy validated schedule snapshots into PostgreSQL.

## Architecture and source layout

```text
generator -> ZeroMQ -> ingestion -> InfluxDB 3 Core
                                      |
dashboard <- HTTP/SSE <- FastAPI -----+---- PostgreSQL
```

`app/routes/` defines HTTP contracts, `app/services/` contains reusable API logic, `app/repositories/` owns relational queries, `app/database/` owns clients, `app/workers/` owns reconciliation/evaluation loops, and `database/` contains ordered, non-destructive SQL migrations. Permanent design material is under `docs/`.

Requirements are Python 3.12, PostgreSQL 17 and InfluxDB 3 Core. Runtime Python packages are pinned in `requirements.txt`; no broker or frontend package is required by this repository.

## Install, configure and migrate

```bash
cd /home/shaurya/Downloads/adwiser-backend
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Set `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`, `INFLUX_URL`, `INFLUX_DATABASE` and the read-capable `INFLUX_TOKEN`. `HOST` and `PORT` are application defaults; pass the same values to Uvicorn. `CORS_ORIGINS` is an optional comma-separated allow-list. Do not commit `.env` or print secrets in diagnostics.

Apply every migration once, in numeric order. They are written to tolerate a repeat where practical, but migration state should still be recorded by the operator.

```bash
for migration in database/*.sql; do
  PGPASSWORD='local-secret' psql -h 127.0.0.1 -p 5433 \
    -U adwiser -d adwiser_config -v ON_ERROR_STOP=1 -f "$migration" || exit 1
done
```

Migration 009 extends schedule identity from `(site, version)` to `(site, generator-run, version)` without deleting history. It also stores total slots, used percentage and payload size.

## Accounts, roles and recovery

Passwords use Argon2; sessions are random HttpOnly cookies stored only as SHA-256 hashes and expire after 12 hours. `superadmin` receives all permissions. `operator` and `engineer` permissions are stored in `role_permission` and can be changed through Administration. Every query and write is scoped to a site.

Use an existing superadmin and `POST /api/v1/adwiser/admin/users` for normal account creation. For first installation, generate an Argon2 hash locally with `app.utils.passwords.hash_password`, then insert the user with a unique ID and email through `psql`; never place the plaintext password in SQL history. The same controlled procedure can replace `password_hash`, increment `session_version`, and delete that user's `user_session` rows for recovery.

Permissions include `scheduler.view`, `details.engineering`, `mac.full`, `resolution.1s`, alert actions, notes, CSV export and `admin.manage`. Disabled accounts and changed session versions invalidate existing sessions.

## API contract

Monitor base path is `/api/v1/adwiser`. The 24 operations are:

- Session: `POST`, `GET`, `DELETE /session`.
- Administration: `GET/POST /admin/users`, `POST /admin/users/{id}`, `POST /admin/users/{id}/delete`, `GET /admin/roles`, and `POST /admin/roles/{role}`.
- Views: `GET /overview`, `/aps`, `/aps/{id}`, `/clients`, `/clients/{id}`, `/flows`, `/flows/{id}`, `/alerts`, `/alerts/{id}`, `/scheduler`, `/events`.
- Actions: `POST /alerts/{id}/ack`, `/alerts/{id}/resolve`, `/alerts/{id}/notes`, and `/events/{id}/notes`.

Monitor views accept `site` and `range` (`live`, `15m`, `1h`, `6h`, `24h`, `7d`, `custom`). Custom `from`/`to` values are UTC epoch milliseconds. Results use `{ "meta": ..., "data": ... }`; `null` means unavailable, never zero. Clients send `credentials: 'include'`. JSON actions require `Content-Type: application/json`. `/scheduler` requires `scheduler.view`.

Signal base path is `/api/v1`: `GET /filters`, `GET /dashboard/performance` and `GET /stream/performance`. The last is SSE: it emits `config`, then a `snapshot` every ten seconds, or `unavailable`. Signal is intentionally read-only and currently public, so expose it only on an appropriate network.

Interactive signatures are available at `/docs` and `/openapi.json`. See `docs/api-guide.md` for client behaviour and error handling.

## Data ownership and workers

InfluxDB contains raw 20 ms telemetry and ingestion-owned health measurements. Historical API queries select raw rows and time-bucket them at request time. There are deliberately no roll-ups or automatic retention policies in this repository. Long ranges therefore depend on raw-data availability and can be expensive.

PostgreSQL stores sites, floors, AP/radio/client/flow inventory, SLA classes, users, sessions, permissions, rules, alert lifecycle, events/notes and sampled schedule versions. Inventory reconciliation runs every 60 seconds. Alert evaluation runs every ten seconds under an advisory lock; missing telemetry does not clear a condition, and a partial unique index prevents duplicate open alerts.

The schedule worker recovers recent `schedule_snapshot` rows at startup, then uses an Influx timestamp cursor so large JSON payloads are not reread on every poll. It rejects malformed dimensions, out-of-bounds allocations, overlaps on one radio, time/slot disagreement, and references absent from current inventory. Exact `(site_id, run_id, schedule_ver)` duplicates are ignored.

The Scheduler response couples version metadata and allocations from the same stored row. The simulation has parallel radios, while the immutable dashboard has one row per AP; the API therefore selects and explicitly labels one representative radio per AP. It never serializes concurrent radio allocations into a false sequential timeline. Full radio allocations remain in PostgreSQL.

Signal snapshots use an eight-second cache and per-key lock to coalesce concurrent requests. A failed refresh may serve cached data for up to five minutes with stale metadata; Monitor responses are not cached and carry `Cache-Control: no-store`.

## Run and operate

```bash
cd /home/shaurya/Downloads/adwiser-backend
env -u INFLUX_TOKEN .venv/bin/uvicorn app.main:app \
  --host 127.0.0.1 --port 3001

curl -i http://127.0.0.1:3001/health
curl -i http://127.0.0.1:3001/ready
curl -i http://127.0.0.1:3001/api/v1/filters
```

Unset a stale shell `INFLUX_TOKEN` when `.env` should be authoritative. Startup checks PostgreSQL and InfluxDB; telemetry routes honestly report unavailability if InfluxDB cannot be reached. SIGINT/SIGTERM stops workers, closes the HTTP client and drains the PostgreSQL pool.

For another machine, bind Uvicorn to `0.0.0.0` only behind a firewall or reverse proxy, point database variables at actual remote addresses, configure explicit CORS origins, and use HTTPS so session cookies remain protected. See `docs/deployment.md` for the complete three-device procedure.

## Performance and limits

The live schedule payload averaged about 15.7 KiB (13.2–17.5 KiB observed). At a ten-second snapshot cadence that is about 132 MiB/day of uncompressed JSON before PostgreSQL/Influx overhead; actual storage must be measured on the target filesystem. A loaded-host 20-second dry run completed 1,000 cycles with zero missed slots, 14 deadline overruns, 33 MiB maximum RSS, 43% of one CPU core and 20.66 seconds wall time. This is a short observation, not a sustained guarantee.

Known limits: Wi-Fi and controller measurements are synthetic; raw history has no roll-up tier; Signal is public; ZeroMQ transport is not durable; AP channel and flow five-tuple remain unavailable until emitted/stored; the frontend can show only a representative radio; and floor-plan display size cannot be capped through backend metadata without changing physical dimensions. The protected dashboard was therefore left unchanged. See `docs/architecture.md`, `docs/scheduler.md`, `docs/telemetry-mapping.md`, and `docs/deployment.md`.

Troubleshooting priorities are `/health`, backend logs, PostgreSQL connectivity, Influx authentication/database name, inventory worker counts, scheduler worker rejections, generator/ingestion clocks, and firewall reachability. Never fix a missing metric by substituting an arbitrary constant.
