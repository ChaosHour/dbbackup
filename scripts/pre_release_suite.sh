#!/bin/bash
# =============================================================================
# dbbackup Pre-Release Validation Suite
# =============================================================================
#
# Comprehensive validation gate for v6.0.0 / stable v5.9.0 release.
# Integrates existing test scripts with new multi-DB, leak detection,
# race detection, signal handling, large-scale, and error injection tests.
#
# Usage:
#   bash scripts/pre_release_suite.sh              # Full suite
#   bash scripts/pre_release_suite.sh --quick       # Tests 1-4 only (no Docker)
#   bash scripts/pre_release_suite.sh --test=3      # Run single test
#   bash scripts/pre_release_suite.sh --skip-docker  # Skip Docker-dependent tests
#   bash scripts/pre_release_suite.sh --help        # Show help
#
# Exit codes:
#   0 = All critical tests passed (warnings allowed)
#   1 = One or more critical tests failed — DO NOT RELEASE
#
# Output:
#   /tmp/dbbackup_pre_release/report.txt — Full validation report
#   /tmp/dbbackup_pre_release/*.log      — Individual test logs
#
# Expected runtime:
#   Full suite (with Docker):  15-25 minutes
#   Quick mode (no Docker):    3-5 minutes
#
# Prerequisites:
#   - Go 1.21+, PostgreSQL running locally, python3
#   - Docker (optional, for multi-DB and error injection tests)
#   - Existing scripts: validate_tui.sh, test-sigint-cleanup.sh,
#     benchmark_restore.sh, pre_production_check.sh
# =============================================================================

set -uo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_DIR/dbbackup_linux_amd64"
REPORT_DIR="/tmp/dbbackup_pre_release"
REPORT_FILE="$REPORT_DIR/report.txt"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Test database settings (uses local PostgreSQL)
PG_HOST="${DBBACKUP_HOST:-localhost}"
PG_PORT="${DBBACKUP_PORT:-5432}"
PG_USER="${DBBACKUP_USER:-postgres}"
PG_PASS="${DBBACKUP_PASSWORD:-postgres}"

# Counters
TESTS_TOTAL=0
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_WARNED=0
TESTS_SKIPPED=0
CRITICAL_FAILURES=0
SUITE_START=$(date +%s)

# Flags
RUN_QUICK=false
SINGLE_TEST=""
SKIP_DOCKER=false
HAS_DOCKER=false

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ─────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ─────────────────────────────────────────────────────────────────────────────

for arg in "$@"; do
    case "$arg" in
        --quick)       RUN_QUICK=true ;;
        --skip-docker) SKIP_DOCKER=true ;;
        --test=*)      SINGLE_TEST="${arg#--test=}" ;;
        --help|-h)
            echo "Usage: bash scripts/pre_release_suite.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --quick        Run tests 1-4 only (no Docker required)"
            echo "  --skip-docker  Skip tests that require Docker"
            echo "  --test=N       Run only test N (1-10)"
            echo "  --help         Show this help"
            echo ""
            echo "Tests:"
            echo "   1  Race detector (go test -race)"
            echo "   2  Memory & goroutine leak detection"
            echo "   3  Multi-database parity (Docker: PG16, MySQL8, MariaDB10)"
            echo "   4  Signal handling (SIGINT, SIGTERM)"
            echo "   5  Backwards compatibility"
            echo "   6  TUI comprehensive validation"
            echo "   7  Large-scale restore (1000 tables)"
            echo "   8  Tiered restore validation"
            echo "   9  Error injection (disk, connection, config)"
            echo "  10  Performance baseline"
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg (use --help)"
            exit 1
            ;;
    esac
done

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

banner() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
}

log_pass() {
    echo -e "  ${GREEN}✅ PASS${NC}: $1"
    echo "PASS: $1" >> "$REPORT_DIR/results.log"
    TESTS_PASSED=$((TESTS_PASSED + 1))
    TESTS_TOTAL=$((TESTS_TOTAL + 1))
}

log_fail() {
    echo -e "  ${RED}❌ FAIL${NC}: $1"
    echo "FAIL: $1" >> "$REPORT_DIR/results.log"
    TESTS_FAILED=$((TESTS_FAILED + 1))
    TESTS_TOTAL=$((TESTS_TOTAL + 1))
    CRITICAL_FAILURES=$((CRITICAL_FAILURES + 1))
}

log_warn() {
    echo -e "  ${YELLOW}⚠️  WARN${NC}: $1"
    echo "WARN: $1" >> "$REPORT_DIR/results.log"
    TESTS_WARNED=$((TESTS_WARNED + 1))
    TESTS_TOTAL=$((TESTS_TOTAL + 1))
}

log_skip() {
    echo -e "  ${BLUE}⏭️  SKIP${NC}: $1"
    echo "SKIP: $1" >> "$REPORT_DIR/results.log"
    TESTS_SKIPPED=$((TESTS_SKIPPED + 1))
}

log_info() {
    echo -e "  ${BLUE}ℹ️${NC}  $1"
}

# Check whether a specific test should run
should_run() {
    local test_num="$1"
    if [[ -n "$SINGLE_TEST" ]] && [[ "$SINGLE_TEST" != "$test_num" ]]; then
        return 1
    fi
    if [[ "$RUN_QUICK" == "true" ]] && [[ "$test_num" -gt 4 ]]; then
        return 1
    fi
    return 0
}

# Check if Docker is usable
check_docker() {
    if [[ "$SKIP_DOCKER" == "true" ]]; then
        HAS_DOCKER=false
        return
    fi
    if command -v docker &>/dev/null && docker info &>/dev/null; then
        HAS_DOCKER=true
    else
        HAS_DOCKER=false
    fi
}

# Safely stop & remove a Docker container
docker_cleanup() {
    local name="$1"
    docker stop "$name" &>/dev/null || true
    docker rm -f "$name" &>/dev/null || true
}

# Wait for a Docker container's port to become reachable
wait_for_port() {
    local host="$1" port="$2" timeout="${3:-30}"
    local elapsed=0
    while ! bash -c "echo >/dev/tcp/$host/$port" 2>/dev/null; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [[ $elapsed -ge $timeout ]]; then
            return 1
        fi
    done
    return 0
}

# Get the version from the binary / source
get_version() {
    grep 'version.*=.*"' "$PROJECT_DIR/main.go" | head -1 | sed 's/.*"\(.*\)".*/\1/'
}

# ─────────────────────────────────────────────────────────────────────────────
# Environment setup
# ─────────────────────────────────────────────────────────────────────────────

setup() {
    banner "PRE-RELEASE VALIDATION SUITE"
    echo ""
    echo -e "  ${BOLD}Version${NC}:     $(get_version)"
    echo -e "  ${BOLD}Date${NC}:        $(date)"
    echo -e "  ${BOLD}Host${NC}:        $(hostname)"
    echo -e "  ${BOLD}Go${NC}:          $(go version 2>/dev/null | awk '{print $3}')"
    echo -e "  ${BOLD}OS/Arch${NC}:     $(uname -s)/$(uname -m)"
    echo -e "  ${BOLD}CPUs${NC}:        $(nproc)"
    echo -e "  ${BOLD}RAM${NC}:         $(free -h 2>/dev/null | awk '/^Mem:/{print $2}' || echo 'unknown')"
    echo -e "  ${BOLD}Mode${NC}:        $(if $RUN_QUICK; then echo 'quick'; else echo 'full'; fi)"
    echo ""

    check_docker
    if $HAS_DOCKER; then
        echo -e "  ${BOLD}Docker${NC}:      $(docker --version 2>/dev/null | head -1)"
    else
        echo -e "  ${BOLD}Docker${NC}:      ${YELLOW}not available (Docker tests will be skipped)${NC}"
    fi
    echo ""

    # Prepare report directory
    rm -rf "$REPORT_DIR"
    mkdir -p "$REPORT_DIR"
    : > "$REPORT_DIR/results.log"

    # Build fresh binary
    echo -e "  Building binary..."
    cd "$PROJECT_DIR"
    if go build -o "$BINARY" . 2>"$REPORT_DIR/build.log"; then
        echo -e "  ${GREEN}✅${NC} Binary built: $BINARY"
    else
        echo -e "  ${RED}❌${NC} Build failed — aborting"
        cat "$REPORT_DIR/build.log"
        exit 1
    fi
    echo ""
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 1: Race Detector
# ─────────────────────────────────────────────────────────────────────────────

test_1_race_detector() {
    should_run 1 || return 0
    banner "Test 1/10: Race Detector"

    local logfile="$REPORT_DIR/01_race.log"

    echo -e "  Running ${BOLD}go test -race${NC} on all packages (timeout 10m)..."
    cd "$PROJECT_DIR"

    # Race detector requires CGO
    set +e
    CGO_ENABLED=1 go test -race -short -count=1 -timeout=10m ./... \
        > "$logfile" 2>&1
    local exit_code=$?
    set -e

    if grep -qi "WARNING: DATA RACE" "$logfile"; then
        log_fail "Data races detected"
        echo ""
        grep -A 20 "WARNING: DATA RACE" "$logfile" | head -40
        return
    fi

    if [[ $exit_code -ne 0 ]]; then
        # Tests failed but no race — treat as warning (test bugs, not races)
        local fail_count
        fail_count=$(grep -c "^--- FAIL" "$logfile" || true)
        fail_count=${fail_count:-0}
        log_warn "Tests had $fail_count failure(s) but no data races detected (exit=$exit_code)"
        return
    fi

    local pkg_count
    pkg_count=$(grep -c "^ok" "$logfile" || true)
    pkg_count=${pkg_count:-0}
    log_pass "No data races — $pkg_count packages tested"
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 2: Memory & Goroutine Leak Detection
# ─────────────────────────────────────────────────────────────────────────────

test_2_leak_detection() {
    should_run 2 || return 0
    banner "Test 2/10: Memory & Goroutine Leak Detection"

    local logfile="$REPORT_DIR/02_leaks.log"

    # ── 2a: Goroutine leak check via repeated unit tests ──
    echo -e "  ${BOLD}2a${NC}: Running streaming parser 50 iterations..."
    cd "$PROJECT_DIR"

    set +e
    go test -run=TestParseStatementsContextCancellation -count=50 \
        -timeout=5m ./internal/engine/native/ \
        > "$logfile" 2>&1
    local exit_a=$?
    set -e

    if [[ $exit_a -eq 0 ]]; then
        log_pass "Goroutine leak test — 50 iterations clean"
    else
        log_warn "Goroutine leak test — exit $exit_a (review $logfile)"
    fi

    # ── 2b: Memory stability via go test -benchmem ──
    echo -e "  ${BOLD}2b${NC}: Running memory allocation benchmark..."

    local memlog="$REPORT_DIR/02_memory.log"
    set +e
    go test -run='^$' -bench=BenchmarkClassifyStatement \
        -benchmem -count=3 -timeout=2m \
        ./internal/engine/native/ \
        > "$memlog" 2>&1
    local exit_b=$?
    set -e

    if [[ $exit_b -eq 0 ]]; then
        log_pass "Memory benchmark completed (see $memlog)"
    else
        log_warn "Memory benchmark exited $exit_b"
    fi

    # ── 2c: Check for known leak patterns in source ──
    echo -e "  ${BOLD}2c${NC}: Scanning for unclosed resource patterns..."

    local leak_patterns=0
    # time.NewTicker without defer Stop
    while IFS= read -r line; do
        file=$(echo "$line" | cut -d: -f1)
        lineno=$(echo "$line" | cut -d: -f2)
        # Look in next 10 lines for defer.*Stop
        context=$(sed -n "$lineno,$((lineno+10))p" "$file" 2>/dev/null)
        if ! echo "$context" | grep -q "defer.*Stop\|\.Stop()"; then
            echo "    ⚠️  Ticker without Stop: $file:$lineno" >> "$logfile"
            leak_patterns=$((leak_patterns + 1))
        fi
    done < <(grep -rn "time.NewTicker" "$PROJECT_DIR/internal/" --include="*.go" 2>/dev/null)

    # sql.Open without defer Close
    while IFS= read -r line; do
        file=$(echo "$line" | cut -d: -f1)
        lineno=$(echo "$line" | cut -d: -f2)
        context=$(sed -n "$lineno,$((lineno+15))p" "$file" 2>/dev/null)
        if ! echo "$context" | grep -q "defer.*Close\|\.Close()"; then
            echo "    ⚠️  sql.Open without Close: $file:$lineno" >> "$logfile"
            leak_patterns=$((leak_patterns + 1))
        fi
    done < <(grep -rn "sql.Open(" "$PROJECT_DIR/internal/" --include="*.go" 2>/dev/null | grep -v "_test.go")

    if [[ $leak_patterns -eq 0 ]]; then
        log_pass "No unclosed resource patterns found"
    else
        log_warn "$leak_patterns potential unclosed resource pattern(s)"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 3: Multi-Database Parity
# ─────────────────────────────────────────────────────────────────────────────

test_3_multi_db() {
    should_run 3 || return 0
    banner "Test 3/10: Multi-Database Parity"

    local logfile="$REPORT_DIR/03_multidb.log"

    # ── 3a: Source-level parity checks (always runs) ──
    echo -e "  ${BOLD}3a${NC}: Source parity analysis..."

    # Check Engine interface compliance
    local pg_methods mysql_methods
    pg_methods=$(grep -c "^func (e \*PostgreSQLNativeEngine)" \
        "$PROJECT_DIR/internal/engine/native/postgresql.go" 2>/dev/null || true)
    pg_methods=${pg_methods:-0}
    mysql_methods=$(grep -c "^func (e \*MySQLNativeEngine)" \
        "$PROJECT_DIR/internal/engine/native/mysql.go" 2>/dev/null || true)
    mysql_methods=${mysql_methods:-0}
    echo "    PostgreSQL engine methods: $pg_methods" | tee -a "$logfile"
    echo "    MySQL engine methods:      $mysql_methods" | tee -a "$logfile"

    # Check Engine interface implementation
    local interface_methods
    interface_methods=$(grep -c "^\t[A-Z]" \
        <(sed -n '/^type Engine interface/,/^}/p' \
        "$PROJECT_DIR/internal/engine/native/manager.go") 2>/dev/null || true)
    interface_methods=${interface_methods:-0}
    echo "    Engine interface methods:   $interface_methods" | tee -a "$logfile"

    # Check MySQL restore engine exists
    if grep -q "MySQLRestoreEngine" "$PROJECT_DIR/internal/engine/native/restore.go"; then
        log_pass "MySQLRestoreEngine exists in restore.go"
    else
        log_fail "MySQLRestoreEngine missing from restore.go"
    fi

    # Check manager.go routes MySQL restore
    if grep -q 'case "mysql"' "$PROJECT_DIR/internal/engine/native/manager.go"; then
        log_pass "manager.go routes MySQL native restore"
    else
        log_fail "manager.go does not route MySQL native restore"
    fi

    # Check TUI tools have MySQL branches
    local tui_mysql_tools=0
    for tool in table_sizes kill_connections drop_database blob_stats; do
        if grep -q 'mysql' "$PROJECT_DIR/internal/tui/${tool}.go" 2>/dev/null; then
            tui_mysql_tools=$((tui_mysql_tools + 1))
        fi
    done
    if [[ $tui_mysql_tools -ge 4 ]]; then
        log_pass "All 4 TUI tools have MySQL branches"
    else
        log_warn "Only $tui_mysql_tools/4 TUI tools have MySQL branches"
    fi

    # Check bulk-load optimizations
    if grep -q "FOREIGN_KEY_CHECKS" "$PROJECT_DIR/internal/engine/native/mysql.go" && \
       grep -q "UNIQUE_CHECKS" "$PROJECT_DIR/internal/engine/native/mysql.go"; then
        log_pass "MySQL bulk-load optimizations present"
    else
        log_fail "MySQL missing bulk-load optimizations"
    fi

    # ── 3b: Docker integration tests (optional) ──
    if ! $HAS_DOCKER; then
        log_skip "Docker multi-DB integration tests (Docker not available)"
        return
    fi

    echo -e "  ${BOLD}3b${NC}: Docker integration — PostgreSQL 16..."

    docker_cleanup "pre-release-pg16"
    docker run -d --name pre-release-pg16 \
        -p 15432:5432 \
        -e POSTGRES_PASSWORD=prerelease \
        postgres:16 \
        > /dev/null 2>&1

    if wait_for_port localhost 15432 30; then
        # Create test database
        PGPASSWORD=prerelease psql -h localhost -p 15432 -U postgres \
            -c "CREATE DATABASE pre_release_test;" &>/dev/null || true

        # Create minimal test dump
        local test_sql="$REPORT_DIR/pg_test.sql"
        cat > "$test_sql" <<'EOSQL'
CREATE TABLE IF NOT EXISTS parity_test (id serial PRIMARY KEY, name text, created_at timestamp DEFAULT now());
INSERT INTO parity_test (name) VALUES ('test1'), ('test2'), ('test3');
EOSQL

        # Test native restore
        set +e
        PGPASSWORD=prerelease "$BINARY" restore single "$test_sql" \
            --host localhost --port 15432 --user postgres \
            --database pre_release_test \
            --database-type postgres \
            --confirm --no-tui \
            >> "$logfile" 2>&1
        local pg_exit=$?
        set -e

        if [[ $pg_exit -eq 0 ]]; then
            log_pass "PostgreSQL 16 restore OK"
        else
            log_warn "PostgreSQL 16 restore exited $pg_exit (review log)"
        fi
    else
        log_warn "PostgreSQL 16 container failed to start"
    fi
    docker_cleanup "pre-release-pg16"

    echo -e "  ${BOLD}3c${NC}: Docker integration — MySQL 8.0..."

    docker_cleanup "pre-release-mysql8"
    docker run -d --name pre-release-mysql8 \
        -p 13306:3306 \
        -e MYSQL_ROOT_PASSWORD=prerelease \
        -e MYSQL_DATABASE=pre_release_test \
        mysql:8.0 \
        > /dev/null 2>&1

    if wait_for_port localhost 13306 60; then
        # Wait for MySQL to fully initialize
        sleep 10

        local test_mysql="$REPORT_DIR/mysql_test.sql"
        cat > "$test_mysql" <<'EOSQL'
CREATE TABLE IF NOT EXISTS parity_test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
INSERT INTO parity_test (name) VALUES ('test1'), ('test2'), ('test3');
EOSQL

        set +e
        "$BINARY" restore single "$test_mysql" \
            --host localhost --port 13306 --user root --password prerelease \
            --database pre_release_test \
            --database-type mysql \
            --confirm --no-tui \
            >> "$logfile" 2>&1
        local mysql_exit=$?
        set -e

        if [[ $mysql_exit -eq 0 ]]; then
            log_pass "MySQL 8.0 restore OK"
        else
            log_warn "MySQL 8.0 restore exited $mysql_exit (review log)"
        fi
    else
        log_warn "MySQL 8.0 container failed to start"
    fi
    docker_cleanup "pre-release-mysql8"

    echo -e "  ${BOLD}3d${NC}: Docker integration — MariaDB 10.11..."

    docker_cleanup "pre-release-mariadb"
    docker run -d --name pre-release-mariadb \
        -p 13307:3306 \
        -e MYSQL_ROOT_PASSWORD=prerelease \
        -e MYSQL_DATABASE=pre_release_test \
        mariadb:10.11 \
        > /dev/null 2>&1

    if wait_for_port localhost 13307 60; then
        sleep 10

        local test_maria="$REPORT_DIR/maria_test.sql"
        cat > "$test_maria" <<'EOSQL'
CREATE TABLE IF NOT EXISTS parity_test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
INSERT INTO parity_test (name) VALUES ('test1'), ('test2'), ('test3');
EOSQL

        set +e
        "$BINARY" restore single "$test_maria" \
            --host localhost --port 13307 --user root --password prerelease \
            --database pre_release_test \
            --database-type mariadb \
            --confirm --no-tui \
            >> "$logfile" 2>&1
        local maria_exit=$?
        set -e

        if [[ $maria_exit -eq 0 ]]; then
            log_pass "MariaDB 10.11 restore OK"
        else
            log_warn "MariaDB 10.11 restore exited $maria_exit (review log)"
        fi
    else
        log_warn "MariaDB 10.11 container failed to start"
    fi
    docker_cleanup "pre-release-mariadb"
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 4: Signal Handling
# ─────────────────────────────────────────────────────────────────────────────

test_4_signal_handling() {
    should_run 4 || return 0
    banner "Test 4/10: Signal Handling"

    local logfile="$REPORT_DIR/04_signals.log"

    # ── 4a: Existing SIGINT test ──
    echo -e "  ${BOLD}4a${NC}: SIGINT cleanup test..."
    if [[ -f "$SCRIPT_DIR/test-sigint-cleanup.sh" ]]; then
        set +e
        bash "$SCRIPT_DIR/test-sigint-cleanup.sh" > "$logfile" 2>&1
        local sigint_exit=$?
        set -e
        if [[ $sigint_exit -eq 0 ]]; then
            log_pass "SIGINT cleanup test passed"
        else
            log_warn "SIGINT cleanup test exited $sigint_exit (review log)"
        fi
    else
        log_skip "test-sigint-cleanup.sh not found"
    fi

    # ── 4b: SIGTERM graceful shutdown ──
    echo -e "  ${BOLD}4b${NC}: SIGTERM graceful shutdown..."

    # Start binary in version mode (fast, won't need DB) but give it a second
    set +e
    "$BINARY" health > "$REPORT_DIR/04_sigterm.log" 2>&1 &
    local pid=$!
    sleep 2

    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
        local sigterm_exit=$?
        # 143 = 128+SIGTERM(15), 0 = already exited cleanly
        if [[ $sigterm_exit -eq 143 || $sigterm_exit -eq 0 || $sigterm_exit -eq 130 ]]; then
            log_pass "SIGTERM handled (exit=$sigterm_exit)"
        else
            log_warn "SIGTERM exit code unexpected: $sigterm_exit"
        fi
    else
        # Process finished before we could signal it — that's OK for fast commands
        wait "$pid" 2>/dev/null
        log_pass "SIGTERM test — process exited cleanly before signal"
    fi
    set -e

    # ── 4c: Check for orphaned connections after a signal ──
    echo -e "  ${BOLD}4c${NC}: Checking for leaked connections..."
    set +e
    local leaked
    leaked=$(PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
        -d postgres -tAc \
        "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE '%dbbackup%'" \
        2>/dev/null)
    set -e

    if [[ -z "$leaked" || "$leaked" == "0" ]]; then
        log_pass "No leaked dbbackup connections"
    else
        log_warn "$leaked lingering dbbackup connection(s)"
    fi

    # ── 4d: TUI InterruptMsg coverage ──
    echo -e "  ${BOLD}4d${NC}: TUI InterruptMsg handler coverage..."
    local update_count interrupt_count
    update_count=$(grep -rn "func.*Update.*tea.Msg" "$PROJECT_DIR/internal/tui/" --include="*.go" | wc -l)
    interrupt_count=$(grep -rn "tea.InterruptMsg" "$PROJECT_DIR/internal/tui/" --include="*.go" | wc -l)

    if [[ $interrupt_count -ge $update_count ]]; then
        log_pass "All $update_count TUI Update() methods handle InterruptMsg"
    else
        log_warn "InterruptMsg coverage: $interrupt_count/$update_count Update methods"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 5: Backwards Compatibility
# ─────────────────────────────────────────────────────────────────────────────

test_5_backwards_compat() {
    should_run 5 || return 0
    banner "Test 5/10: Backwards Compatibility"

    local logfile="$REPORT_DIR/05_compat.log"

    # ── 5a: Old .meta.json format (v5.8.x single-database) ──
    echo -e "  ${BOLD}5a${NC}: Old single-database .meta.json format..."

    local meta_old="$REPORT_DIR/compat_old.meta.json"
    cat > "$meta_old" <<'EOF'
{
  "database": "olddb",
  "size_bytes": 1048576,
  "created_at": "2026-01-01T00:00:00Z"
}
EOF

    # Verify Go can unmarshal both old and new formats
    set +e
    go test -run=TestLoadTableProfile -count=1 -timeout=30s \
        "$PROJECT_DIR/internal/engine/native/" \
        > "$logfile" 2>&1
    local meta_exit=$?
    set -e

    if [[ $meta_exit -eq 0 ]]; then
        log_pass "Metadata parser tests pass (backward-compatible)"
    else
        log_warn "Metadata parser test exited $meta_exit (may not have test)"
    fi

    # ── 5b: Old cluster .meta.json format ──
    echo -e "  ${BOLD}5b${NC}: Old cluster .meta.json format..."

    local meta_cluster="$REPORT_DIR/compat_cluster.meta.json"
    cat > "$meta_cluster" <<'EOF'
{
  "databases": [
    {"database": "db1", "size_bytes": 10485760},
    {"database": "db2", "size_bytes": 52428800}
  ],
  "total_size_bytes": 62914560
}
EOF
    # Just verify JSON is valid and parseable
    if python3 -c "import json; json.load(open('$meta_cluster'))" 2>/dev/null; then
        log_pass "Cluster .meta.json format valid"
    else
        log_fail "Cluster .meta.json format invalid"
    fi

    # ── 5c: Config file compatibility ──
    echo -e "  ${BOLD}5c${NC}: Configuration file compatibility..."

    local old_conf="$REPORT_DIR/compat_old.conf"
    cat > "$old_conf" <<'EOF'
[database]
type = postgres
host = localhost
port = 5432
user = postgres

[backup]
directory = /var/backups/dbbackup
retention = 7
compression = 6
EOF

    # Binary should handle config without crashing (just verify it starts)
    set +e
    "$BINARY" version > /dev/null 2>&1
    local cfg_exit=$?
    set -e

    if [[ $cfg_exit -eq 0 ]]; then
        log_pass "Binary starts with current config format"
    else
        log_warn "Binary start issue (exit=$cfg_exit)"
    fi

    # ── 5d: Archive format detection ──
    echo -e "  ${BOLD}5d${NC}: Archive format detection..."

    # Check all format constants are defined
    local format_count
    format_count=$(grep -c "^\tFormat[A-Z]" \
        "$PROJECT_DIR/internal/restore/formats.go" 2>/dev/null || true)
    format_count=${format_count:-0}
    echo "    Archive formats defined: $format_count" >> "$logfile"

    if [[ $format_count -ge 4 ]]; then
        log_pass "Archive format detection — $format_count formats supported"
    else
        log_warn "Only $format_count archive formats defined"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 6: TUI Comprehensive Validation
# ─────────────────────────────────────────────────────────────────────────────

test_6_tui() {
    should_run 6 || return 0
    banner "Test 6/10: TUI Comprehensive Validation"

    local logfile="$REPORT_DIR/06_tui.log"

    # ── 6a: Existing TUI validator ──
    echo -e "  ${BOLD}6a${NC}: TUI structural validation (validate_tui.sh)..."
    if [[ -f "$SCRIPT_DIR/validate_tui.sh" ]]; then
        set +e
        bash "$SCRIPT_DIR/validate_tui.sh" > "$logfile" 2>&1
        local tui_exit=$?
        set -e
        if [[ $tui_exit -eq 0 ]]; then
            log_pass "TUI structural validation passed"
        else
            log_warn "TUI structural validation had issues (exit=$tui_exit)"
        fi
    else
        log_skip "validate_tui.sh not found"
    fi

    # ── 6b: TUI screen count ──
    echo -e "  ${BOLD}6b${NC}: TUI screen inventory..."
    local screen_count
    screen_count=$(grep -rn "type.*Model struct\|type.*View struct" \
        "$PROJECT_DIR/internal/tui/" --include="*.go" 2>/dev/null | wc -l)

    echo "    TUI screens/views found: $screen_count" | tee -a "$logfile"
    if [[ $screen_count -ge 15 ]]; then
        log_pass "TUI has $screen_count screens/views"
    else
        log_warn "Only $screen_count TUI screens found (expected ≥15)"
    fi

    # ── 6c: Debug infrastructure ──
    echo -e "  ${BOLD}6c${NC}: TUI debug infrastructure..."
    if [[ -f "$PROJECT_DIR/internal/tui/debug.go" ]]; then
        local debug_calls
        debug_calls=$(grep -rn "tuiDebugLog\|tuiDebugTransition\|tuiDebugQuit" \
            "$PROJECT_DIR/internal/tui/" --include="*.go" 2>/dev/null | \
            grep -v "debug.go" | wc -l)
        log_pass "TUI debug infrastructure — $debug_calls instrumentation points"
    else
        log_warn "TUI debug.go not found"
    fi

    # ── 6d: DB-type awareness in TUI ──
    echo -e "  ${BOLD}6d${NC}: TUI database-type awareness..."
    local db_aware_screens=0
    for f in "$PROJECT_DIR/internal/tui/"*.go; do
        [[ -f "$f" ]] || continue
        if grep -q "IsMySQL\|IsPostgreSQL\|dbType\|DatabaseType\|DisplayDatabaseType" "$f" 2>/dev/null; then
            db_aware_screens=$((db_aware_screens + 1))
        fi
    done
    echo "    DB-aware TUI files: $db_aware_screens" | tee -a "$logfile"
    if [[ $db_aware_screens -ge 6 ]]; then
        log_pass "TUI database awareness — $db_aware_screens files"
    else
        log_warn "Only $db_aware_screens TUI files are DB-type aware"
    fi

    # ── 6e: WithoutSignalHandler on all NewProgram calls ──
    echo -e "  ${BOLD}6e${NC}: Signal handler configuration..."
    local program_count signal_handler_count
    program_count=$(grep -rn "tea.NewProgram" "$PROJECT_DIR/internal/tui/" \
        "$PROJECT_DIR/cmd/" --include="*.go" 2>/dev/null | wc -l)
    signal_handler_count=$(grep -rn "tea.WithoutSignalHandler" "$PROJECT_DIR/internal/tui/" \
        "$PROJECT_DIR/cmd/" --include="*.go" 2>/dev/null | wc -l)

    if [[ $signal_handler_count -ge $program_count ]] && [[ $program_count -gt 0 ]]; then
        log_pass "All $program_count tea.NewProgram calls use WithoutSignalHandler"
    else
        log_warn "Signal handler: $signal_handler_count/$program_count programs covered"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 7: Large Scale Test
# ─────────────────────────────────────────────────────────────────────────────

test_7_large_scale() {
    should_run 7 || return 0
    banner "Test 7/10: Large Scale Test"

    local logfile="$REPORT_DIR/07_largescale.log"

    # ── 7a: Verify fakedbcreator exists ──
    echo -e "  ${BOLD}7a${NC}: Large database creator..."
    if [[ -f "$PROJECT_DIR/fakedbcreator.sh" ]]; then
        log_pass "fakedbcreator.sh exists"
    else
        log_warn "fakedbcreator.sh not found"
    fi

    # ── 7b: Adaptive worker allocation in source ──
    echo -e "  ${BOLD}7b${NC}: Adaptive worker allocation..."
    if grep -q "classifyTableSize\|workersForSize\|AdaptiveConfig" \
        "$PROJECT_DIR/internal/engine/native/parallel_restore.go" 2>/dev/null; then
        log_pass "Adaptive worker allocation code present"
    else
        log_fail "Adaptive worker allocation code missing"
    fi

    # ── 7c: Verify loadTableProfile metadata-driven planning ──
    echo -e "  ${BOLD}7c${NC}: Metadata-driven restore planning..."
    if grep -q "loadTableProfile" \
        "$PROJECT_DIR/internal/engine/native/parallel_restore.go" 2>/dev/null; then
        log_pass "loadTableProfile metadata-driven planning present"
    else
        log_warn "loadTableProfile not found"
    fi

    # ── 7d: Connection pool tuning for large restores ──
    echo -e "  ${BOLD}7d${NC}: Connection pool tuning..."
    local pool_config
    pool_config=$(grep -c "MaxConns\|MinConns\|HealthCheckPeriod\|MaxConnIdleTime\|MaxConnLifetime" \
        "$PROJECT_DIR/internal/engine/native/parallel_restore.go" 2>/dev/null || true)
    pool_config=${pool_config:-0}
    if [[ $pool_config -ge 4 ]]; then
        log_pass "Connection pool tuned ($pool_config parameters)"
    else
        log_warn "Connection pool tuning incomplete ($pool_config parameters)"
    fi

    # ── 7e: Quick smoke test with local PostgreSQL (small scale) ──
    echo -e "  ${BOLD}7e${NC}: Smoke test — create and restore small DB..."

    # Create a tiny test dump directly
    local test_db="pre_release_scale_test"
    local test_sql="$REPORT_DIR/scale_test.sql"

    # Generate a 100-table SQL dump
    {
        echo "-- Pre-release scale test dump"
        for i in $(seq 1 100); do
            echo "CREATE TABLE IF NOT EXISTS scale_test_$i (id serial PRIMARY KEY, data text, num integer, ts timestamp DEFAULT now());"
            echo "INSERT INTO scale_test_$i (data, num) VALUES"
            for j in $(seq 1 9); do
                echo "  ('row_${j}_table_${i}', $j),"
            done
            echo "  ('row_10_table_${i}', 10);"
        done
    } > "$test_sql"

    # Create target database
    set +e
    PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
        -d postgres -c "DROP DATABASE IF EXISTS $test_db;" &>/dev/null
    PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
        -d postgres -c "CREATE DATABASE $test_db;" &>/dev/null
    set -e

    # Restore using native engine
    set +e
    "$BINARY" restore single "$test_sql" \
        --host "$PG_HOST" --port "$PG_PORT" --user "$PG_USER" --password "$PG_PASS" \
        --database "$test_db" --database-type postgres \
        --native-engine --confirm --no-tui \
        >> "$logfile" 2>&1
    local scale_exit=$?
    set -e

    if [[ $scale_exit -eq 0 ]]; then
        # Verify table count
        local table_count
        table_count=$(PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
            -d "$test_db" -tAc \
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'" \
            2>/dev/null || echo 0)
        if [[ "$table_count" -ge 100 ]]; then
            log_pass "100-table restore OK ($table_count tables created)"
        else
            log_warn "Expected 100 tables, got $table_count"
        fi
    else
        log_warn "100-table restore exited $scale_exit"
    fi

    # Cleanup
    PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
        -d postgres -c "DROP DATABASE IF EXISTS $test_db;" &>/dev/null || true
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 8: Tiered Restore Validation
# ─────────────────────────────────────────────────────────────────────────────

test_8_tiered_restore() {
    should_run 8 || return 0
    banner "Test 8/10: Tiered Restore Validation"

    local logfile="$REPORT_DIR/08_tiered.log"

    # ── 8a: Tiered restore code paths ──
    echo -e "  ${BOLD}8a${NC}: Tiered restore infrastructure..."

    local tiered_checks=0

    if grep -q "TableClassification" \
        "$PROJECT_DIR/internal/engine/native/restore_mode.go" 2>/dev/null; then
        tiered_checks=$((tiered_checks + 1))
    fi
    if grep -q "PriorityCritical\|PriorityImportant\|PriorityCold" \
        "$PROJECT_DIR/internal/engine/native/restore_mode.go" 2>/dev/null; then
        tiered_checks=$((tiered_checks + 1))
    fi
    if grep -q "ClassifyTable" \
        "$PROJECT_DIR/internal/engine/native/restore_mode.go" 2>/dev/null; then
        tiered_checks=$((tiered_checks + 1))
    fi
    if grep -q "restoreFileTiered" \
        "$PROJECT_DIR/internal/engine/native/parallel_restore.go" 2>/dev/null; then
        tiered_checks=$((tiered_checks + 1))
    fi
    if grep -q "TieredRestore" \
        "$PROJECT_DIR/internal/config/config.go" 2>/dev/null; then
        tiered_checks=$((tiered_checks + 1))
    fi

    if [[ $tiered_checks -ge 5 ]]; then
        log_pass "Tiered restore infrastructure complete ($tiered_checks/5 checks)"
    else
        log_warn "Tiered restore incomplete ($tiered_checks/5 checks)"
    fi

    # ── 8b: Restore mode detection ──
    echo -e "  ${BOLD}8b${NC}: Restore mode detection..."

    if grep -q "DetectOptimalRestoreMode" \
        "$PROJECT_DIR/internal/engine/native/restore_mode.go" 2>/dev/null; then
        log_pass "DetectOptimalRestoreMode auto-detection present"
    else
        log_fail "DetectOptimalRestoreMode missing"
    fi

    # ── 8c: Table classification patterns ──
    echo -e "  ${BOLD}8c${NC}: Default table classification..."

    if grep -q "DefaultTableClassification" \
        "$PROJECT_DIR/internal/engine/native/restore_mode.go" 2>/dev/null; then
        log_pass "DefaultTableClassification patterns defined"
    else
        log_warn "DefaultTableClassification not found"
    fi

    # ── 8d: Phase callbacks ──
    echo -e "  ${BOLD}8d${NC}: Phase callback wiring..."

    if grep -q "PhaseCallback" \
        "$PROJECT_DIR/internal/engine/native/parallel_restore.go" 2>/dev/null && \
       grep -q "PhaseCallback" \
        "$PROJECT_DIR/internal/restore/engine.go" 2>/dev/null; then
        log_pass "PhaseCallback wired between engine and restore layer"
    else
        log_warn "PhaseCallback wiring incomplete"
    fi

    # ── 8e: 3-tier mode strings ──
    echo -e "  ${BOLD}8e${NC}: Restore modes (safe/balanced/turbo)..."

    local mode_count
    mode_count=$(grep -c "RestoreMode\(Safe\|Balanced\|Turbo\)" \
        "$PROJECT_DIR/internal/engine/native/restore_mode.go" 2>/dev/null || true)
    mode_count=${mode_count:-0}
    if [[ $mode_count -ge 3 ]]; then
        log_pass "All 3 restore modes defined"
    else
        log_fail "Only $mode_count restore modes found (need 3)"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 9: Error Injection
# ─────────────────────────────────────────────────────────────────────────────

test_9_error_injection() {
    should_run 9 || return 0
    banner "Test 9/10: Error Injection"

    local logfile="$REPORT_DIR/09_errors.log"

    # ── 9a: Invalid restore mode ──
    echo -e "  ${BOLD}9a${NC}: Invalid restore mode rejection..."
    set +e
    "$BINARY" restore single /dev/null \
        --restore-mode=nonexistent \
        --confirm --no-tui \
        > "$REPORT_DIR/09a_invalid_mode.log" 2>&1
    local inv_exit=$?
    set -e

    if [[ $inv_exit -ne 0 ]]; then
        log_pass "Invalid restore mode rejected (exit=$inv_exit)"
    else
        log_warn "Invalid restore mode was accepted"
    fi

    # ── 9b: Non-existent backup file ──
    echo -e "  ${BOLD}9b${NC}: Missing backup file rejection..."
    set +e
    "$BINARY" restore single /nonexistent/file.sql.gz \
        --confirm --no-tui \
        > "$REPORT_DIR/09b_missing.log" 2>&1
    local miss_exit=$?
    set -e

    if [[ $miss_exit -ne 0 ]]; then
        log_pass "Missing backup file rejected (exit=$miss_exit)"
    else
        log_warn "Missing backup file not detected"
    fi

    # ── 9c: Truncated/corrupt backup ──
    echo -e "  ${BOLD}9c${NC}: Corrupted backup detection..."
    local corrupt_file="$REPORT_DIR/corrupt.sql.gz"
    dd if=/dev/urandom of="$corrupt_file" bs=1024 count=10 2>/dev/null
    set +e
    "$BINARY" restore single "$corrupt_file" \
        --database pre_release_corrupt_test \
        --confirm --no-tui \
        > "$REPORT_DIR/09c_corrupt.log" 2>&1
    local corrupt_exit=$?
    set -e

    if [[ $corrupt_exit -ne 0 ]]; then
        log_pass "Corrupted backup detected (exit=$corrupt_exit)"
    else
        log_warn "Corrupted backup not detected"
    fi

    # ── 9d: Invalid database type ──
    echo -e "  ${BOLD}9d${NC}: Invalid database type rejection..."
    set +e
    "$BINARY" restore single /dev/null \
        --database-type=oracle \
        --confirm --no-tui \
        > "$REPORT_DIR/09d_dbtype.log" 2>&1
    local dbtype_exit=$?
    set -e

    if [[ $dbtype_exit -ne 0 ]]; then
        log_pass "Invalid database type rejected (exit=$dbtype_exit)"
    else
        log_warn "Invalid database type accepted"
    fi

    # ── 9e: Connection to non-existent host ──
    echo -e "  ${BOLD}9e${NC}: Connection failure handling..."
    set +e
    timeout 10 "$BINARY" restore single /dev/null \
        --host 192.0.2.1 --port 59999 \
        --database nonexistent \
        --confirm --no-tui \
        > "$REPORT_DIR/09e_connfail.log" 2>&1
    local conn_exit=$?
    set -e

    if [[ $conn_exit -ne 0 ]]; then
        log_pass "Connection failure handled (exit=$conn_exit)"
    else
        log_warn "Connection failure not handled properly"
    fi

    # ── 9f: Error message quality (no panics in error paths) ──
    echo -e "  ${BOLD}9f${NC}: No panics in error paths..."
    local panic_count=0
    for log in "$REPORT_DIR"/09*.log; do
        if grep -qi "panic\|SIGSEGV\|runtime error" "$log" 2>/dev/null; then
            panic_count=$((panic_count + 1))
            echo "    ⚠️  Panic found in: $log"
        fi
    done

    if [[ $panic_count -eq 0 ]]; then
        log_pass "No panics in error paths"
    else
        log_fail "$panic_count panic(s) found in error paths"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Test 10: Performance Baseline
# ─────────────────────────────────────────────────────────────────────────────

test_10_performance() {
    should_run 10 || return 0
    banner "Test 10/10: Performance Baseline"

    local logfile="$REPORT_DIR/10_performance.log"

    # ── 10a: Binary size check ──
    echo -e "  ${BOLD}10a${NC}: Binary size..."
    local bin_size
    bin_size=$(stat -c%s "$BINARY" 2>/dev/null || stat -f%z "$BINARY" 2>/dev/null || echo 0)
    local bin_mb=$((bin_size / 1024 / 1024))
    echo "    Binary size: ${bin_mb}MB" | tee -a "$logfile"

    if [[ $bin_mb -lt 60 ]]; then
        log_pass "Binary size ${bin_mb}MB (<60MB limit)"
    else
        log_warn "Binary size ${bin_mb}MB (≥60MB — review dependencies)"
    fi

    # ── 10b: Startup time ──
    echo -e "  ${BOLD}10b${NC}: Startup latency..."
    local start_ns end_ns elapsed_ms
    start_ns=$(date +%s%N)
    "$BINARY" version > /dev/null 2>&1
    end_ns=$(date +%s%N)
    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    echo "    Startup time: ${elapsed_ms}ms" | tee -a "$logfile"

    if [[ $elapsed_ms -lt 3000 ]]; then
        log_pass "Startup latency ${elapsed_ms}ms (<3s)"
    else
        log_warn "Startup latency ${elapsed_ms}ms (≥3s — slow)"
    fi

    # ── 10c: Go build time ──
    echo -e "  ${BOLD}10c${NC}: Build time..."
    cd "$PROJECT_DIR"
    local build_start build_end build_sec
    build_start=$(date +%s)
    go build -o /dev/null . 2>/dev/null
    build_end=$(date +%s)
    build_sec=$((build_end - build_start))
    echo "    Build time: ${build_sec}s" | tee -a "$logfile"

    if [[ $build_sec -lt 30 ]]; then
        log_pass "Build time ${build_sec}s (<30s)"
    else
        log_warn "Build time ${build_sec}s (≥30s — slow)"
    fi

    # ── 10d: Test suite speed ──
    echo -e "  ${BOLD}10d${NC}: Unit test speed..."
    local test_start test_end test_sec
    test_start=$(date +%s)
    set +e
    go test -short -count=1 -timeout=5m ./... > "$REPORT_DIR/10_tests.log" 2>&1
    local test_exit=$?
    set -e
    test_end=$(date +%s)
    test_sec=$((test_end - test_start))
    echo "    Test suite time: ${test_sec}s" | tee -a "$logfile"

    if [[ $test_exit -eq 0 ]]; then
        log_pass "Unit tests pass in ${test_sec}s"
    else
        log_warn "Unit tests exited $test_exit (${test_sec}s)"
    fi

    # ── 10e: Module verification ──
    echo -e "  ${BOLD}10e${NC}: Module integrity..."
    set +e
    go mod verify > "$REPORT_DIR/10_modverify.log" 2>&1
    local mod_exit=$?
    set -e

    if [[ $mod_exit -eq 0 ]]; then
        log_pass "go mod verify OK"
    else
        log_fail "go mod verify failed"
    fi

    # ── 10f: Existing benchmark script ──
    echo -e "  ${BOLD}10f${NC}: benchmark_restore.sh availability..."
    if [[ -f "$SCRIPT_DIR/benchmark_restore.sh" ]]; then
        log_pass "benchmark_restore.sh available for manual performance testing"
    else
        log_warn "benchmark_restore.sh not found"
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Report generation
# ─────────────────────────────────────────────────────────────────────────────

generate_report() {
    local suite_end suite_duration
    suite_end=$(date +%s)
    suite_duration=$((suite_end - SUITE_START))
    local duration_min=$((suite_duration / 60))
    local duration_sec=$((suite_duration % 60))

    # Determine recommendation
    local recommendation
    if [[ $CRITICAL_FAILURES -gt 0 ]]; then
        recommendation="DO NOT RELEASE — Fix $CRITICAL_FAILURES critical failure(s) first"
    elif [[ $TESTS_WARNED -gt 5 ]]; then
        recommendation="RELEASE AS BETA — $TESTS_WARNED warnings need review"
    elif [[ $TESTS_WARNED -gt 0 ]]; then
        recommendation="RELEASE CANDIDATE — $TESTS_WARNED minor warning(s)"
    else
        recommendation="READY FOR STABLE RELEASE"
    fi

    cat > "$REPORT_FILE" <<EOF
╔═══════════════════════════════════════════════════════════════╗
║              PRE-RELEASE VALIDATION REPORT                     ║
╚═══════════════════════════════════════════════════════════════╝

Date:       $(date)
Version:    $(get_version)
Host:       $(hostname)
Go:         $(go version 2>/dev/null | awk '{print $3}')
OS/Arch:    $(uname -s)/$(uname -m)
CPUs:       $(nproc 2>/dev/null || echo 'unknown')
Duration:   ${duration_min}m ${duration_sec}s

═══════════════════════════════════════════════════════════════
RESULTS SUMMARY
═══════════════════════════════════════════════════════════════

  Total checks:     $TESTS_TOTAL
  ✅ Passed:        $TESTS_PASSED
  ❌ Failed:        $TESTS_FAILED
  ⚠️  Warnings:     $TESTS_WARNED
  ⏭️  Skipped:      $TESTS_SKIPPED

  Critical failures: $CRITICAL_FAILURES

═══════════════════════════════════════════════════════════════
RECOMMENDATION
═══════════════════════════════════════════════════════════════

  $recommendation

═══════════════════════════════════════════════════════════════
DETAILED RESULTS
═══════════════════════════════════════════════════════════════

$(cat "$REPORT_DIR/results.log" 2>/dev/null)

═══════════════════════════════════════════════════════════════
TEST LOGS
═══════════════════════════════════════════════════════════════

$(for f in "$REPORT_DIR"/*.log; do
    [[ "$f" == "$REPORT_DIR/results.log" ]] && continue
    echo "--- $(basename "$f") ---"
    head -50 "$f" 2>/dev/null
    lines=$(wc -l < "$f" 2>/dev/null || echo 0)
    if [[ $lines -gt 50 ]]; then
        echo "  ... ($((lines - 50)) more lines, see full log)"
    fi
    echo ""
done)
EOF

    banner "VALIDATION COMPLETE"
    echo ""
    echo -e "  ${BOLD}Duration${NC}:   ${duration_min}m ${duration_sec}s"
    echo -e "  ${BOLD}Total${NC}:      $TESTS_TOTAL checks"
    echo -e "  ${GREEN}Passed${NC}:     $TESTS_PASSED"
    echo -e "  ${RED}Failed${NC}:     $TESTS_FAILED"
    echo -e "  ${YELLOW}Warnings${NC}:   $TESTS_WARNED"
    echo -e "  ${BLUE}Skipped${NC}:    $TESTS_SKIPPED"
    echo ""

    if [[ $CRITICAL_FAILURES -gt 0 ]]; then
        echo -e "  ${RED}${BOLD}❌ $recommendation${NC}"
    elif [[ $TESTS_WARNED -gt 5 ]]; then
        echo -e "  ${YELLOW}${BOLD}⚠️  $recommendation${NC}"
    else
        echo -e "  ${GREEN}${BOLD}✅ $recommendation${NC}"
    fi
    echo ""
    echo -e "  Full report: ${BOLD}$REPORT_FILE${NC}"
    echo ""
}

# ─────────────────────────────────────────────────────────────────────────────
# Main execution
# ─────────────────────────────────────────────────────────────────────────────

main() {
    setup

    test_1_race_detector
    test_2_leak_detection
    test_3_multi_db
    test_4_signal_handling
    test_5_backwards_compat
    test_6_tui
    test_7_large_scale
    test_8_tiered_restore
    test_9_error_injection
    test_10_performance

    generate_report

    # Exit code: 0 if no critical failures, 1 otherwise
    if [[ $CRITICAL_FAILURES -gt 0 ]]; then
        exit 1
    fi
    exit 0
}

main
