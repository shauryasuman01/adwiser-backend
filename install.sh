
#!/usr/bin/env bash

set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
source ./prerequisites.sh

adwiser_prerequisites ingestion

umask 077

PYTHON_BIN="${PYTHON_BIN:-python3.12}"

INGESTION_DIR="${INGESTION_DIR:-$HOME/adwiser-ingestion}"

mkdir -p .adwiser-runtime

exec 9>.adwiser-runtime/lock
flock -x 9

echo "======================================"
echo "     ADWISER Backend Installation"
echo "======================================"

# 1. Check prerequisites
echo "[1/8] Checking prerequisites..."

for cmd in "$PYTHON_BIN" docker curl; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "ERROR: Missing prerequisite: $cmd"
        exit 1
    fi
done

docker info >/dev/null

for file in requirements.txt .env.example \
    app/main.py app/utils/create_admin.py; do

    if [[ ! -f "$file" ]]; then
        echo "ERROR: Missing $file"
        exit 1
    fi
done

if [[ ! -f "$INGESTION_DIR/.env" ]]; then
    echo "ERROR: Ingestion configuration not found."
    echo "Install ADWISER Ingestion first."
    exit 1
fi

# Prevent credentials from being committed
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then

    if git ls-files --error-unmatch .env >/dev/null 2>&1; then
        echo "ERROR: .env is tracked by Git."
        exit 1
    fi

    if ! git check-ignore -q .env; then
        echo "ERROR: Add .env to .gitignore first."
        exit 1
    fi
fi

# 2. Create virtual environment
echo "[2/8] Preparing virtual environment..."

if [[ ! -x .venv/bin/python ]]; then
    "$PYTHON_BIN" -m venv .venv
else
    echo "Virtual environment already exists."
fi

# 3. Install dependencies
echo "[3/8] Installing dependencies..."

.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip check

# 4-7. Configure PostgreSQL, credentials and migrations
.venv/bin/python - "$INGESTION_DIR" "$@" <<'PY'

import asyncio
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
import time

from pathlib import Path

import asyncpg
import httpx

from dotenv import dotenv_values

CONTAINER = "adwiser-postgres"
IMAGE = "postgres:17"
VOLUME = "adwiser-postgres-data"
DATA_DIR = "/var/lib/postgresql/data"

DB_NAME = "adwiser_config"
DB_USER = "adwiser"
DB_HOST = "127.0.0.1"
DB_PORT = 5433

FLOOR_MIGRATION = "008_pilot_demo_floor.sql"

# Reviewed migration checksums for safe adoption
# of the original manually installed backend.
REVIEWED = {
    "001_inventory.sql":
        "1333a13c95d83b24f2a3e4948af20ac18b0b3c6f60d0a26aa1fd8ee0e1d55fc2",
    "002_authentication.sql":
        "901764c91ade54589ca765833b6f834277eb5b4397055fc4a485d2b2cd2013",
    "003_sessions.sql":
        "5cbb8b5b287afbb302dfa053e7373bab00ccd30f267fc839018ba6c2180d74dc",
    "004_audit_alert_base.sql":
        "1d89940be20289412472799c506979cf4fb1a4c7cf0440f04bf2d0235ca46f82",
    "005_notes_schedule.sql":
        "aa9187d0039503217b70d315e33f9999e2a10a731b370b826565191204b412fc",
    "006_alert_rules.sql":
        "d29261d7d516accc87ce7debe9d5fd9d3911f8b2e0630106b840ec588b276a18",
    "007_login_throttle.sql":
        "1f995d47536a2f984cf22c05e213dd8565847761ffe7d41d794c6f646acd0fb1",
    "009_schedule_run_identity.sql":
        "23e60d4c7400cc25ec75936a74eb9af8296feae35e4a02b82916e5d0b4eef803",
}

ingestion_dir = Path(sys.argv[1])

args = sys.argv[2:]

if any(arg != "--adopt-existing" for arg in args):
    sys.exit("Usage: bash install.sh [--adopt-existing]")

adopt_existing = "--adopt-existing" in args


def docker(*args, env=None):
    return subprocess.run(
        ["docker", *args],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        env=env,
    ).stdout.strip()


def exists(resource_type, name):
    return subprocess.run(
        ["docker", resource_type, "inspect", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode == 0


def valid_secret(value):
    return bool(
        value
        and not value.startswith("replace-with-")
        and not value.startswith("YOUR_")
    )


# 4. Read existing configuration
print("[4/8] Preparing configuration...", flush=True)

env_path = Path(".env")

if not env_path.exists():
    env_path.write_text(
        Path(".env.example").read_text()
    )

env_path.chmod(0o600)

backend = dotenv_values(
    env_path, interpolate=False
)

ingestion = dotenv_values(
    ingestion_dir / ".env",
    interpolate=False,
)

influx_url = ingestion.get("INFLUXDB3_URL")
influx_database = ingestion.get("INFLUXDB3_DATABASE")
influx_token = ingestion.get("INFLUXDB3_AUTH_TOKEN")

if not influx_url or not influx_database:
    sys.exit("ERROR: Missing InfluxDB configuration.")

if not valid_secret(influx_token):
    sys.exit("ERROR: Invalid ingestion token.")

# Refuse to modify non-default PostgreSQL setups
expected = {
    "PG_HOST": DB_HOST,
    "PG_PORT": str(DB_PORT),
    "PG_DATABASE": DB_NAME,
    "PG_USER": DB_USER,
}

for key, value in expected.items():
    if backend.get(key) not in (None, "", value):
        sys.exit(
            f"ERROR: Non-default PostgreSQL setting: {key}"
        )

container_exists = exists("container", CONTAINER)
volume_exists = exists("volume", VOLUME)

pg_password = backend.get("PG_PASSWORD")

if not valid_secret(pg_password):

    if container_exists or volume_exists:
        sys.exit(
            "ERROR: Existing PostgreSQL storage found, "
            "but its password is missing from .env. "
            "Restore the original password."
        )

    pg_password = secrets.token_hex(32)

    print("Secure PostgreSQL password generated.")

else:
    print("Reusing existing PostgreSQL password.")

# Preserve an existing separate read token when possible.
old_token = backend.get("INFLUX_TOKEN")

if (
    valid_secret(old_token)
    and backend.get("INFLUX_URL") == influx_url
    and backend.get("INFLUX_DATABASE") == influx_database
):
    influx_token = old_token

settings = {
    "HOST": "127.0.0.1",
    "PORT": "3001",
    **expected,
    "PG_PASSWORD": pg_password,
    "INFLUX_URL": influx_url,
    "INFLUX_DATABASE": influx_database,
    "INFLUX_TOKEN": influx_token,
    "CORS_ORIGINS": backend.get("CORS_ORIGINS")
        or "http://localhost:5173",
}

# Save configuration atomically without printing secrets
lines = [
    line
    for line in env_path.read_text().splitlines()
    if not any(
        re.match(
            r"^\s*(export\s+)?" + key + r"\s*=",
            line,
        )
        for key in settings
    )
]

for key, value in settings.items():
    lines.append(
        key + "=" + json.dumps(value)
    )

with tempfile.NamedTemporaryFile(
    mode="w",
    dir=".",
    delete=False,
) as f:
    f.write("\n".join(lines) + "\n")
    temp_path = Path(f.name)

temp_path.chmod(0o600)
os.replace(temp_path, env_path)

# Override stale shell environment variables
os.environ.update(settings)

print("Backend .env configured securely.")

# 5. Create or reuse PostgreSQL
print("[5/8] Preparing PostgreSQL...", flush=True)

if not container_exists:

    docker("volume", "create", VOLUME)

    # Pass password through environment, not command arguments
    docker_env = os.environ.copy()

    docker_env.update({
        "POSTGRES_USER": DB_USER,
        "POSTGRES_PASSWORD": pg_password,
        "POSTGRES_DB": DB_NAME,
    })

    docker(
        "run", "-d",
        "--name", CONTAINER,
        "--restart", "unless-stopped",
        "-e", "POSTGRES_USER",
        "-e", "POSTGRES_PASSWORD",
        "-e", "POSTGRES_DB",
        "-p", "127.0.0.1:5433:5432",
        "-v", f"{VOLUME}:{DATA_DIR}",
        IMAGE,
        env=docker_env,
    )

    print("PostgreSQL container created.")

else:

    info = json.loads(
        docker("inspect", CONTAINER)
    )[0]

    mount_ok = any(
        m.get("Name") == VOLUME
        and m["Destination"] == DATA_DIR
        for m in info["Mounts"]
    )

    bindings = info["HostConfig"][
        "PortBindings"
    ].get("5432/tcp", [])

    port_ok = any(
        b["HostIp"] == DB_HOST
        and b["HostPort"] == str(DB_PORT)
        for b in bindings
    )

    if not mount_ok or not port_ok:
        sys.exit(
            "ERROR: Unexpected PostgreSQL container configuration."
        )

    if not info["State"]["Running"]:
        docker("start", CONTAINER)

    print("Reusing existing PostgreSQL container.")


async def connect_postgres():

    deadline = time.monotonic() + 60

    while True:

        try:
            return await asyncpg.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=pg_password,
                database=DB_NAME,
                timeout=5,
            )

        except (
            OSError,
            asyncio.TimeoutError,
            asyncpg.CannotConnectNowError,
        ):

            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "PostgreSQL did not become ready."
                )

            await asyncio.sleep(1)


async def migrate(conn):

    # Prevent concurrent installers from applying migrations
    await conn.execute(
        "SELECT pg_advisory_lock(729318640)"
    )

    try:

        existing_schema = await conn.fetchval("""
            SELECT
                to_regclass('public.app_user') IS NOT NULL
                OR
                to_regclass('public.site') IS NOT NULL
        """)

        ledger_exists = await conn.fetchval("""
            SELECT to_regclass(
                'public.adwiser_install_migration'
            ) IS NOT NULL
        """)

        files = sorted(
            Path("database").glob("*.sql")
        )

        if not set(REVIEWED).issubset(
            {file.name for file in files}
        ):
            raise RuntimeError(
                "Expected SQL migrations are missing."
            )

        if existing_schema and not ledger_exists:

            if not adopt_existing:
                raise RuntimeError(
                    "Existing manual installation detected. "
                    "Back up PostgreSQL and run "
                    "bash install.sh --adopt-existing"
                )

            # Only replay migrations reviewed for adoption
            for file in files:

                if file.name == FLOOR_MIGRATION:
                    continue

                checksum = hashlib.sha256(
                    file.read_bytes()
                ).hexdigest()

                if REVIEWED.get(file.name) != checksum:
                    raise RuntimeError(
                        f"Unreviewed migration: {file.name}. "
                        "Review before adopting existing data."
                    )

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS
                adwiser_install_migration (
                    filename TEXT PRIMARY KEY,
                    sha256 TEXT NOT NULL,
                    applied_at TIMESTAMPTZ DEFAULT now()
                )
        """)

        for file in files:

            # Demo floor requires synchronized inventory
            if file.name == FLOOR_MIGRATION:
                print("Deferred:", file.name)
                continue

            raw = file.read_bytes()

            checksum = hashlib.sha256(
                raw
            ).hexdigest()

            previous = await conn.fetchval("""
                SELECT sha256
                FROM adwiser_install_migration
                WHERE filename = $1
            """, file.name)

            if previous:

                if previous != checksum:
                    raise RuntimeError(
                        f"Applied migration changed: {file.name}"
                    )

                print("Already applied:", file.name)
                continue

            # Remove outer BEGIN/COMMIT so the migration
            # and its tracking record commit together.
            match = re.fullmatch(
                r"\s*BEGIN\s*;([\s\S]*)COMMIT\s*;\s*",
                raw.decode(),
                flags=re.IGNORECASE,
            )

            if not match:
                raise RuntimeError(
                    f"Unsupported SQL layout: {file.name}"
                )

            async with conn.transaction():

                await conn.execute(
                    match.group(1)
                )

                await conn.execute("""
                    INSERT INTO adwiser_install_migration
                        (filename, sha256)
                    VALUES ($1, $2)
                """, file.name, checksum)

            print("Applied:", file.name)

    finally:
        await conn.execute(
            "SELECT pg_advisory_unlock(729318640)"
        )


async def setup():

    # 6. Verify both database connections
    print("[6/8] Verifying database connections...")

    async with httpx.AsyncClient(timeout=10) as client:

        response = await client.get(
            influx_url.rstrip("/") + "/api/v3/query_sql",
            params={
                "db": influx_database,
                "q": "SELECT 1 AS healthy",
                "format": "json",
            },
            headers={
                "Authorization": "Bearer " + influx_token
            },
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"InfluxDB returned HTTP {response.status_code}"
            )

    print("InfluxDB connection successful.")

    conn = await connect_postgres()

    try:
        print("PostgreSQL connection successful.")

        # 7. Apply migrations
        print("[7/8] Applying database migrations...")

        await migrate(conn)

        existing_admin = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM app_user
                WHERE role = 'superadmin'
            )
        """)

        active_admin = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM app_user
                WHERE role = 'superadmin'
                AND active = true
            )
        """)

        if existing_admin and not active_admin:
            raise RuntimeError(
                "A Super Admin exists but is inactive."
            )

        Path(".adwiser-runtime/admin-required").write_text(
            "no" if active_admin else "yes"
        )

    finally:
        await conn.close()


try:
    asyncio.run(setup())

except RuntimeError as exc:
    sys.exit(f"ERROR: {exc}")

except Exception as exc:
    sys.exit(
        f"ERROR: Database setup failed: {type(exc).__name__}. "
        "Check configuration and database logs."
    )

PY

# 8. Create administrator only if required
echo "[8/8] Checking Super Admin account..."

# Remove stale shell values so the backend reads its .env
unset PG_HOST PG_PORT PG_DATABASE PG_USER PG_PASSWORD
unset INFLUX_URL INFLUX_DATABASE INFLUX_TOKEN

if [[ "$(cat .adwiser-runtime/admin-required)" == "yes" ]]; then

    echo "Creating initial Super Admin account..."

    .venv/bin/python -m app.utils.create_admin

else
    echo "Active Super Admin already exists."
    echo "Existing credentials preserved."
fi

# Verify the administrator exists
ADMIN_OK="$(docker exec adwiser-postgres \
    psql -U adwiser -d adwiser_config -At \
    -c "SELECT EXISTS (
        SELECT 1 FROM app_user
        WHERE role='superadmin' AND active=true
    );")"

if [[ "$ADMIN_OK" != "t" ]]; then
    echo "ERROR: Active Super Admin verification failed."
    exit 1
fi

rm -f .adwiser-runtime/admin-required

echo
echo "======================================"
echo " Backend installation successful!"
echo "======================================"
echo
echo "PostgreSQL: 127.0.0.1:5433"
echo "Database: adwiser_config"
echo "API: http://127.0.0.1:3001"
echo
echo "Start backend: bash start.sh"
echo "Start demo:    bash start.sh --demo-floor"
echo "Stop backend:  bash stop.sh"
