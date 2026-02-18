#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# bench_mariadb.sh — Quick MariaDB benchmark wrapper
#
# Usage:
#   ./scripts/bench_mariadb.sh [database] [iterations]
#
# Environment:
#   MYSQL_PWD      MariaDB password
#   BENCH_HOST     Host (default: localhost)
#   BENCH_PORT     Port (default: 3306)
#   BENCH_USER     User (default: root)
#   BENCH_WORKERS  Parallel workers (default: auto)
#   BENCH_COMP     Compression: gzip|zstd|none (default: gzip)
#   BENCH_SOCKET   Unix socket path (optional)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="${ROOT_DIR}/bin/dbbackup"

# Defaults
DB="${1:-mariadb}"
ITERATIONS="${2:-3}"
HOST="${BENCH_HOST:-localhost}"
PORT="${BENCH_PORT:-3306}"
USER="${BENCH_USER:-root}"
WORKERS="${BENCH_WORKERS:-$(nproc 2>/dev/null || echo 4)}"
COMPRESSION="${BENCH_COMP:-gzip}"
SOCKET="${BENCH_SOCKET:-}"

# Build if needed
if [[ ! -x "$BINARY" ]]; then
    echo "[INFO] Building dbbackup..."
    cd "$ROOT_DIR" && make build
fi

EXTRA_ARGS=()
if [[ -n "$SOCKET" ]]; then
    EXTRA_ARGS+=(--socket "$SOCKET")
fi

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║           MariaDB Benchmark                                 ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Database:    ${DB}"
echo "║  Iterations:  ${ITERATIONS}"
echo "║  Host:        ${HOST}:${PORT}"
echo "║  Workers:     ${WORKERS}"
echo "║  Compression: ${COMPRESSION}"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

exec "$BINARY" benchmark run \
    --db-type mariadb \
    --database "$DB" \
    --host "$HOST" \
    --port "$PORT" \
    --user "$USER" \
    --iterations "$ITERATIONS" \
    --workers "$WORKERS" \
    --compression "$COMPRESSION" \
    --verify \
    --allow-root \
    "${EXTRA_ARGS[@]}" \
    "$@"
