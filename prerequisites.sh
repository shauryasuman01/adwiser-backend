
#!/usr/bin/env bash

# Shared ADWISER prerequisite installer.
# Source this file from install.sh.

adwiser_fail() {
    echo "ERROR: $*" >&2
    exit 1
}

# Ask before installing system packages or changing permissions.
adwiser_approve() {
    local answer

    echo
    echo "$1"

    if ! read -r -p "Continue? [y/N]: " answer < /dev/tty; then
        adwiser_fail "Interactive approval required."
    fi

    case "$answer" in
        y|Y|yes|YES) ;;
        *) adwiser_fail "Installation cancelled." ;;
    esac
}

# Check supported operating system.
adwiser_ubuntu_ok() (
    [[ -f /etc/os-release ]] || exit 1
    source /etc/os-release

    [[ "$ID" == "ubuntu" && "$VERSION_ID" == "24.04" ]]
)

# Install missing Ubuntu packages.
adwiser_apt_install() {

    adwiser_ubuntu_ok ||
        adwiser_fail "Automatic APT setup requires Ubuntu 24.04."

    command -v sudo >/dev/null 2>&1 ||
        adwiser_fail "sudo is required."

    adwiser_approve "Install missing packages: $*"

    sudo apt-get update

    sudo apt-get install -y --no-remove "$@"

    hash -r
}

# Check Python and virtual environment support.
adwiser_python_ok() {
    local python_bin="${PYTHON_BIN:-python3.12}"

    command -v "$python_bin" >/dev/null 2>&1 &&
        "$python_bin" -c '
import sys
import venv
import ensurepip

assert sys.version_info[:2] == (3, 12)
' >/dev/null 2>&1
}

# Check Node.js and npm.
adwiser_node_ok() {

    command -v node >/dev/null 2>&1 &&
        command -v npm >/dev/null 2>&1 &&
        node -e '
process.exit(
    Number(process.versions.node.split(".")[0]) >= 24
    ? 0 : 1
)
' >/dev/null 2>&1 &&
        npm --version >/dev/null 2>&1
}

# Use existing Node.js or ADWISER-managed Node.js.
adwiser_use_node() {

    if adwiser_node_ok; then
        return 0
    fi

    export PATH="$HOME/.local/share/adwiser/node24/bin:$PATH"

    hash -r

    adwiser_node_ok
}

# Install Node.js 24 LTS locally for the current user.
adwiser_install_node() (

    set -Eeuo pipefail

    local parent="$HOME/.local/share/adwiser"
    local destination="$parent/node24"
    local arch
    local archive
    local checksum
    local version
    local temp_dir

    adwiser_approve "Download and install Node.js 24 LTS."

    case "$(uname -m)" in
        x86_64)  arch="x64" ;;
        aarch64) arch="arm64" ;;
        *) adwiser_fail "Unsupported Node.js architecture." ;;
    esac

    if [[ -e "$destination" ]]; then
        adwiser_fail "Existing managed Node.js installation is invalid."
    fi

    mkdir -p "$parent"

    temp_dir="$(mktemp -d "$parent/.node-install.XXXXXX")"

    trap 'rm -rf -- "$temp_dir"' EXIT

    cd "$temp_dir"

    echo "Retrieving Node.js release information..."

    curl -fsSL --retry 3 \
        https://nodejs.org/dist/latest-v24.x/SHASUMS256.txt \
        -o SHASUMS256.txt

    read -r checksum archive < <(
        awk -v arch="$arch" '
            $2 ~ ("^node-v24\\.[0-9]+\\.[0-9]+-linux-" arch "\\.tar\\.xz$") {
                print $1, $2
                exit
            }
        ' SHASUMS256.txt
    )

    [[ "$checksum" =~ ^[a-fA-F0-9]{64}$ ]] ||
        adwiser_fail "Invalid Node.js release checksum."

    version="${archive#node-}"
    version="${version%-linux-${arch}.tar.xz}"

    echo "Downloading Node.js $version..."

    curl -fsSL --retry 3 \
        "https://nodejs.org/dist/$version/$archive" \
        -o "$archive"

    echo "Verifying download checksum..."

    printf '%s  %s\n' "$checksum" "$archive" |
        sha256sum -c -

    mkdir extracted

    tar -xJf "$archive" \
        -C extracted \
        --strip-components=1

    mv extracted "$destination"

    "$destination/bin/node" --version

    echo "Node.js installed successfully."

)

# Install and configure local Docker Engine.
adwiser_prepare_docker() {

    if ! command -v docker >/dev/null 2>&1; then

        adwiser_apt_install docker.io

    fi

    # These installers expect a local Docker Engine.
    if [[ -n "${DOCKER_HOST:-}" &&
          "$DOCKER_HOST" != "unix:///var/run/docker.sock" ]]; then

        adwiser_fail "Remote Docker endpoint detected."

    fi

    if [[ "$(docker context show)" != "default" ]]; then
        adwiser_fail "Select the local default Docker context."
    fi

    if timeout 15s docker info >/dev/null 2>&1; then

        echo "Docker is installed and accessible."
        return 0

    fi

    # Start Docker if its service is stopped.
    if ! systemctl is-active --quiet docker; then

        adwiser_approve "Start the local Docker service."

        sudo systemctl start docker

    fi

    if timeout 15s docker info >/dev/null 2>&1; then

        echo "Docker is accessible."
        return 0

    fi

    # Confirm Docker works with administrator privileges.
    if ! sudo timeout 15s docker \
        --host unix:///var/run/docker.sock \
        info >/dev/null 2>&1; then

        adwiser_fail "Docker daemon is unavailable. Check systemctl status docker."

    fi

    local current_user
    current_user="$(id -un)"

    getent group docker >/dev/null ||
        adwiser_fail "Docker group does not exist."

    if [[ "$(stat -c '%G' /var/run/docker.sock)" != "docker" ]]; then
        adwiser_fail "Unexpected Docker socket permissions."
    fi

    # Add user only if not already a group member.
    if ! id -nG "$current_user" |
        tr ' ' '\n' |
        grep -qx docker; then

        adwiser_approve \
            "Add $current_user to the docker group. This grants root-equivalent Docker access."

        sudo usermod -aG docker "$current_user"

    fi

    echo
    echo "Docker permissions require a new login session."
    echo "Execute:"
    echo
    echo "    newgrp docker"
    echo "    bash install.sh"
    echo
    echo "Alternatively, log out and log back in."

    exit 1
}

# Main prerequisite function.
adwiser_prerequisites() {

    local role="${1:-}"
    local packages=()

    if [[ "$EUID" -eq 0 ]]; then
        adwiser_fail "Run install.sh as a normal user, not with sudo."
    fi

    case "$role" in
        generator|ingestion|backend|dashboard) ;;
        *) adwiser_fail "Invalid repository role: $role" ;;
    esac

    echo
    echo "======================================"
    echo " ADWISER System Prerequisites"
    echo " Repository: $role"
    echo "======================================"

    # Common prerequisites.
    command -v git >/dev/null 2>&1 ||
        packages+=(git)

    command -v flock >/dev/null 2>&1 ||
        packages+=(util-linux)

    # Python repositories.
    if [[ "$role" != "dashboard" ]]; then

        if ! adwiser_python_ok; then

            if [[ "${PYTHON_BIN:-python3.12}" != "python3.12" ]]; then
                adwiser_fail "Custom PYTHON_BIN is unavailable or incompatible."
            fi

            packages+=(python3.12 python3.12-venv)

        fi

    fi

    # HTTP connectivity tools.
    if [[ "$role" != "generator" ]]; then

        command -v curl >/dev/null 2>&1 ||
            packages+=(curl)

        if ! dpkg-query -W -f='${Status}' ca-certificates \
            2>/dev/null | grep -q 'install ok installed'; then

            packages+=(ca-certificates)

        fi

    fi

    # Node.js extraction dependency.
    if [[ "$role" == "dashboard" ]]; then

        if ! adwiser_use_node; then

            command -v xz >/dev/null 2>&1 ||
                packages+=(xz-utils)

        fi

    fi

    # Install missing system dependencies.
    if (( ${#packages[@]} > 0 )); then

        adwiser_apt_install "${packages[@]}"

    else

        echo "Required system packages already available."

    fi

    # Verify Python.
    if [[ "$role" != "dashboard" ]]; then

        adwiser_python_ok ||
            adwiser_fail "Python 3.12 or venv support is unavailable."

        "${PYTHON_BIN:-python3.12}" --version

    fi

    # Docker is required by ingestion and backend.
    if [[ "$role" == "ingestion" ||
          "$role" == "backend" ]]; then

        adwiser_prepare_docker

        docker --version

    fi

    # Node.js is required by dashboard.
    if [[ "$role" == "dashboard" ]]; then

        if ! adwiser_use_node; then

            adwiser_install_node

            adwiser_use_node ||
                adwiser_fail "Node.js installation verification failed."

        fi

        echo "Node.js: $(node --version)"
        echo "npm: $(npm --version)"

    fi

    echo
    echo "All system prerequisites verified."
    echo "======================================"
    echo
}
