
#!/usr/bin/env bash

set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

umask 077

CONTAINER="adwiser-postgres"

RUNTIME_DIR=".adwiser-runtime"
PID_FILE="$RUNTIME_DIR/backend.pid"
LOG_FILE="$RUNTIME_DIR/service.log"

DEMO_FLOOR=false

for arg in "$@"; do
    case "$arg" in
        --demo-floor)
            DEMO_FLOOR=true
            ;;
        *)
            echo "Unknown argument: $arg"
            exit 1
            ;;
    esac
done

mkdir -p "$RUNTIME_DIR"

exec 9>"$RUNTIME_DIR/lock"
flock -x 9

process_identity() {
    local stat

    [[ "$1" =~ ^[1-9][0-9]*$ ]] || return 1
    [[ -r "/proc/$1/stat" ]] || return 1

    IFS= read -r stat < "/proc/$1/stat" || return 1

    stat="${stat##*) }"

    set -- $stat

    [[ $# -ge 20 && "$1" != "Z" ]] || return 1

    printf '%s\n' "${20}"
}

echo "Starting ADWISER Backend..."

# Verify installation
if [[ ! -x .venv/bin/python || ! -f .env ]]; then
    echo "ERROR: Run bash install.sh first."
    exit 1
fi

# Remove stale environment variables
unset PG_HOST PG_PORT PG_DATABASE PG_USER PG_PASSWORD
unset INFLUX_URL INFLUX_DATABASE INFLUX_TOKEN
unset HOST PORT CORS_ORIGINS

# Ensure PostgreSQL is running
if ! docker container inspect "$CONTAINER" >/dev/null 2>&1; then

    echo "ERROR: PostgreSQL container does not exist."
    echo "Run bash install.sh"

    exit 1
fi

STATUS="$(docker inspect \
    --format '{{.State.Status}}' "$CONTAINER")"

if [[ "$STATUS" != "running" ]]; then

    echo "Starting PostgreSQL..."

    docker start "$CONTAINER" >/dev/null
fi

# Wait for PostgreSQL readiness
READY=false

for ((i=0; i<60; i++)); do

    if docker exec "$CONTAINER" \
        pg_isready -U adwiser -d adwiser_config \
        >/dev/null 2>&1; then

        READY=true

        break
    fi

    sleep 1
done

if [[ "$READY" != "true" ]]; then

    echo "ERROR: PostgreSQL is not ready."

    exit 1
fi

# Check whether backend is already running
RUNNING=false

if [[ -f "$PID_FILE" ]]; then

    if read -r PID IDENTITY < "$PID_FILE" &&
       [[ -n "$IDENTITY" ]] &&
       [[ "$(process_identity "$PID" || true)" == "$IDENTITY" ]]; then

        RUNNING=true

        echo "Backend is already running. PID: $PID"

    else
        rm -f "$PID_FILE"
    fi
fi

# Launch FastAPI backend
if [[ "$RUNNING" != "true" ]]; then

    nohup .venv/bin/python -u -m uvicorn \
        app.main:app \
        --host 127.0.0.1 \
        --port 3001 \
        >> "$LOG_FILE" 2>&1 < /dev/null 9>&- &

    PID=$!

    IDENTITY="$(process_identity "$PID" || true)"

    if [[ -z "$IDENTITY" ]]; then

        echo "ERROR: Backend failed to start."

        tail -n 20 "$LOG_FILE"

        exit 1
    fi

    printf '%s %s\n' "$PID" "$IDENTITY" > "$PID_FILE"
fi

# Verify backend health
echo "Checking backend health..."

HEALTHY=false

for ((i=0; i<60; i++)); do

    if [[ "$(process_identity "$PID" || true)" != "$IDENTITY" ]]; then

        echo "ERROR: Backend exited."

        rm -f "$PID_FILE"

        tail -n 30 "$LOG_FILE"

        exit 1
    fi

    if curl -fsS --max-time 3 \
        http://127.0.0.1:3001/health \
        >/dev/null 2>&1; then

        HEALTHY=true

        break
    fi

    sleep 1
done

if [[ "$HEALTHY" != "true" ]]; then

    echo "ERROR: Backend health check failed."
    echo "The backend process may still be running."
    echo "Check $LOG_FILE"

    exit 1
fi

echo "Backend health check passed."

# Initialize demo floor after inventory synchronization
if [[ "$DEMO_FLOOR" == "true" ]]; then

    echo "Checking demo floor configuration..."

    .venv/bin/python - <<'PY'

import asyncio
import time

from pathlib import Path

import asyncpg

from app.config import env


async def setup_floor():

    conn = await asyncpg.connect(
        host=env.PG_HOST,
        port=env.PG_PORT,
        database=env.PG_DATABASE,
        user=env.PG_USER,
        password=env.PG_PASSWORD,
    )

    try:

        deadline = time.monotonic() + 120

        while time.monotonic() < deadline:

            ready = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM site
                    WHERE site_id = 'site-01'
                    AND name = 'Pilot Site'
                )
                AND (
                    SELECT COUNT(*)
                    FROM ap
                    WHERE site_id = 'site-01'
                    AND ap_id IN (
                        'ap-01',
                        'ap-02',
                        'ap-03',
                        'ap-04'
                    )
                ) = 4
            """)

            if ready:
                break

            print("Waiting for inventory synchronization...")

            await asyncio.sleep(5)

        else:
            raise RuntimeError(
                "Pilot inventory is not ready. "
                "Ensure generator and ingestion are running. "
                "Then retry bash start.sh --demo-floor."
            )

        migration = Path(
            "database/008_pilot_demo_floor.sql"
        )

        await conn.execute(
            migration.read_text()
        )

        assigned = await conn.fetchval("""
            SELECT COUNT(*)
            FROM ap
            WHERE site_id = 'site-01'
            AND floor_id = 'pilot-demo-floor'
            AND x_m IS NOT NULL
            AND y_m IS NOT NULL
        """)

        print(
            f"Demo floor initialized: "
            f"{assigned}/4 APs assigned."
        )

        if assigned != 4:
            print(
                "Some APs may already have custom "
                "floor assignments or coordinates. "
                "Existing configuration was preserved."
            )

    finally:
        await conn.close()


asyncio.run(setup_floor())

PY

fi

echo
echo "======================================"
echo " ADWISER Backend is running"
echo "======================================"
echo
echo "PID: $PID"
echo "API: http://127.0.0.1:3001"
echo "Documentation: http://127.0.0.1:3001/docs"
echo "Log file: $LOG_FILE"
