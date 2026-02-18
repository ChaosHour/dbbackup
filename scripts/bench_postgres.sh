#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# bench_postgres.sh — Quick PostgreSQL benchmark wrapper
#
# Usage:
#   ./scripts/bench_postgres.sh [database] [iterations]
#
# Environment:
#   PGPASSWORD     PostgreSQL password (or .pgpass)
#   BENCH_HOST     Host (default: localhost)
#   BENCH_PORT     Port (default: 5432)
#   BENCH_USER     User (default: postgres)
#   BENCH_WORKERS  Parallel workers (default: auto)
#   BENCH_COMP     Compression: gzip|zstd|none (default: gzip)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="${ROOT_DIR}/bin/dbbackup"

# Defaults
DB="${1:-postgres}"
ITERATIONS="${2:-3}"
HOST="${BENCH_HOST:-localhost}"
PORT="${BENCH_PORT:-5432}"
USER="${BENCH_USER:-postgres}"
WORKERS="${BENCH_WORKERS:-$(nproc 2>/dev/null || echo 4)}"
COMPRESSION="${BENCH_COMP:-gzip}"

# Build if needed
if [[ ! -x "$BINARY" ]]; then
    echo "[INFO] Building dbbackup..."
    cd "$ROOT_DIR" && make build
fi

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║           PostgreSQL Benchmark                              ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Database:    ${DB}"
echo "║  Iterations:  ${ITERATIONS}"
echo "║  Host:        ${HOST}:${PORT}"
echo "║  Workers:     ${WORKERS}"
echo "║  Compression: ${COMPRESSION}"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

exec "$BINARY" benchmark run \
    --db-type postgres \
    --database "$DB" \
    --host "$HOST" \
    --port "$PORT" \
    --user "$USER" \
    --iterations "$ITERATIONS" \
    --workers "$WORKERS" \
    --compression "$COMPRESSION" \
    --verify \
    --allow-root \
    "$@"
