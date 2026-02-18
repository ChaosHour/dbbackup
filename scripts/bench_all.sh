#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# bench_all.sh — Cross-engine benchmark matrix
#
# Runs benchmarks against all available databases (PostgreSQL, MySQL, MariaDB)
# and prints a comparison table.
#
# Usage:
#   ./scripts/bench_all.sh [iterations]
#
# Environment:
#   PGPASSWORD / MYSQL_PWD  — passwords
#   BENCH_HOST              — host for all engines (default: localhost)
#   BENCH_WORKERS           — parallel workers (default: auto)
#   BENCH_DATABASES         — comma-separated list of databases to benchmark
#                             (default: auto-detect qa_* and bench_* databases)
#
# If no databases are detected, falls back to running against the default
# databases for each engine.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="${ROOT_DIR}/bin/dbbackup"

ITERATIONS="${1:-3}"
HOST="${BENCH_HOST:-localhost}"
WORKERS="${BENCH_WORKERS:-$(nproc 2>/dev/null || echo 4)}"

# Build if needed
if [[ ! -x "$BINARY" ]]; then
    echo "[INFO] Building dbbackup..."
    cd "$ROOT_DIR" && make build
fi

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║           Cross-Engine Benchmark Matrix                     ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Iterations:  ${ITERATIONS}"
echo "║  Host:        ${HOST}"
echo "║  Workers:     ${WORKERS}"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

exec "$BINARY" benchmark matrix \
    --iterations "$ITERATIONS" \
    --host "$HOST" \
    --workers "$WORKERS" \
    --verify \
    --allow-root
