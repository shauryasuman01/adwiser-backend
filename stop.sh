
#!/usr/bin/env bash

set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

umask 077

RUNTIME_DIR=".adwiser-runtime"
PID_FILE="$RUNTIME_DIR/backend.pid"

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

echo "Stopping ADWISER Backend..."

if [[ ! -f "$PID_FILE" ]]; then

    echo "No script-managed backend is running."

    exit 0
fi

if ! read -r PID IDENTITY < "$PID_FILE" ||
   [[ -z "$IDENTITY" ]]; then

    echo "ERROR: Invalid PID file."

    exit 1
fi

if [[ "$(process_identity "$PID" || true)" != "$IDENTITY" ]]; then

    echo "Backend is not running."

    rm -f "$PID_FILE"

    exit 0
fi

# Request graceful shutdown
kill -TERM "$PID"

# Allow background workers to finish
for ((i=0; i<75; i++)); do

    if [[ "$(process_identity "$PID" || true)" != "$IDENTITY" ]]; then

        rm -f "$PID_FILE"

        echo "Backend stopped successfully."
        echo "PostgreSQL and InfluxDB remain running."
        echo "Database data has been preserved."

        exit 0
    fi

    sleep 1
done

echo "ERROR: Backend has not stopped."
echo "Check .adwiser-runtime/service.log"

exit 1
