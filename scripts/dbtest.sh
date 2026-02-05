#!/bin/bash
# Enterprise Database Test Utility
set -e

DB_NAME="${DB_NAME:-testdb_500gb}"
TARGET_GB="${TARGET_GB:-500}"
BLOB_KB="${BLOB_KB:-100}"
BATCH_ROWS="${BATCH_ROWS:-10000}"

show_help() {
    cat << 'HELP'
╔═══════════════════════════════════════════════════════════════╗
║         ENTERPRISE DATABASE TEST UTILITY                      ║
╚═══════════════════════════════════════════════════════════════╝

Usage: ./dbtest.sh <command> [options]

Commands:
  status              Show current database status
  generate            Generate test database (interactive)
  generate-bg         Generate in background (tmux)
  stop                Stop running generation
  drop                Drop test database
  drop-all            Drop ALL non-system databases
  backup              Run dbbackup to SMB
  estimate            Estimate generation time
  log                 Show generation log
  attach              Attach to tmux session

Environment variables:
  DB_NAME=testdb_500gb    Database name
  TARGET_GB=500           Target size in GB
  BLOB_KB=100             Blob size in KB
  BATCH_ROWS=10000        Rows per batch

Examples:
  ./dbtest.sh generate                    # Interactive generation
  TARGET_GB=100 ./dbtest.sh generate-bg   # 100GB in background
  DB_NAME=mytest ./dbtest.sh drop         # Drop specific database
  ./dbtest.sh drop-all                    # Clean slate
HELP
}

cmd_status() {
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║         DATABASE STATUS - $(date '+%Y-%m-%d %H:%M:%S')              ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo ""
    
    echo "┌─ GENERATION ──────────────────────────────────────────────────┐"
    if tmux has-session -t dbgen 2>/dev/null; then
        echo "│ Status: ⏳ RUNNING (attach: ./dbtest.sh attach)"
        echo "│ Log: $(tail -1 /root/generate_500gb.log 2>/dev/null | cut -c1-55)"
    else
        echo "│ Status: ⏹ Not running"
    fi
    echo "└───────────────────────────────────────────────────────────────┘"
    echo ""
    
    echo "┌─ POSTGRESQL DATABASES ─────────────────────────────────────────┐"
    sudo -u postgres psql -t -c "SELECT datname || ': ' || pg_size_pretty(pg_database_size(datname)) FROM pg_database WHERE datname NOT LIKE 'template%' ORDER BY pg_database_size(datname) DESC" 2>/dev/null | sed 's/^/│ /'
    echo "└───────────────────────────────────────────────────────────────┘"
    echo ""
    
    echo "┌─ STORAGE ──────────────────────────────────────────────────────┐"
    echo -n "│ Fast 1TB:  "; df -h /mnt/HC_Volume_104577460 2>/dev/null | awk 'NR==2{print $3"/"$2" ("$5")"}' || echo "N/A"
    echo -n "│ SMB 10TB:  "; df -h /mnt/smb-devdb 2>/dev/null | awk 'NR==2{print $3"/"$2" ("$5")"}' || echo "N/A"
    echo -n "│ Local:     "; df -h / | awk 'NR==2{print $3"/"$2" ("$5")"}'
    echo "└───────────────────────────────────────────────────────────────┘"
}

cmd_stop() {
    echo "Stopping generation..."
    tmux kill-session -t dbgen 2>/dev/null && echo "Stopped." || echo "Not running."
}

cmd_drop() {
    echo "Dropping database: $DB_NAME"
    sudo -u postgres psql -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$DB_NAME' AND pid <> pg_backend_pid();" 2>/dev/null || true
    sudo -u postgres dropdb --if-exists "$DB_NAME" && echo "Dropped: $DB_NAME" || echo "Not found."
}

cmd_drop_all() {
    echo "WARNING: This will drop ALL non-system databases!"
    read -p "Type 'YES' to confirm: " confirm
    [ "$confirm" != "YES" ] && echo "Cancelled." && exit 0
    
    for db in $(sudo -u postgres psql -t -c "SELECT datname FROM pg_database WHERE datname NOT IN ('postgres','template0','template1')"); do
        db=$(echo $db | tr -d ' ')
        [ -n "$db" ] && echo "Dropping: $db" && sudo -u postgres dropdb --if-exists "$db"
    done
    echo "Done."
}

cmd_log() {
    tail -50 /root/generate_500gb.log 2>/dev/null || echo "No log file."
}

cmd_attach() {
    tmux has-session -t dbgen 2>/dev/null && tmux attach -t dbgen || echo "Not running."
}

cmd_backup() {
    mkdir -p /mnt/smb-devdb/cluster-500gb
    dbbackup backup cluster --backup-dir /mnt/smb-devdb/cluster-500gb
}

cmd_estimate() {
    echo "Target: ${TARGET_GB}GB with ${BLOB_KB}KB blobs"
    mins=$((TARGET_GB / 2))
    echo "Estimated: ~${mins} minutes (~$((mins/60)) hours)"
}

cmd_generate() {
    echo "=== Interactive Database Generator ==="
    read -p "Database name [$DB_NAME]: " i; DB_NAME="${i:-$DB_NAME}"
    read -p "Target size GB [$TARGET_GB]: " i; TARGET_GB="${i:-$TARGET_GB}"
    read -p "Blob size KB [$BLOB_KB]: " i; BLOB_KB="${i:-$BLOB_KB}"
    read -p "Rows per batch [$BATCH_ROWS]: " i; BATCH_ROWS="${i:-$BATCH_ROWS}"
    
    echo "Config: $DB_NAME, ${TARGET_GB}GB, ${BLOB_KB}KB blobs"
    read -p "Start? [y/N]: " c
    [[ "$c" != "y" && "$c" != "Y" ]] && echo "Cancelled." && exit 0
    
    do_generate
}

cmd_generate_bg() {
    echo "Starting: $DB_NAME, ${TARGET_GB}GB, ${BLOB_KB}KB blobs"
    tmux kill-session -t dbgen 2>/dev/null || true
    
    tmux new-session -d -s dbgen "DB_NAME=$DB_NAME TARGET_GB=$TARGET_GB BLOB_KB=$BLOB_KB BATCH_ROWS=$BATCH_ROWS /root/dbtest.sh _run 2>&1 | tee /root/generate_500gb.log"
    echo "Started in tmux. Use: ./dbtest.sh log | attach | stop"
}

do_generate() {
    BLOB_BYTES=$((BLOB_KB * 1024))
    echo "=== ${TARGET_GB}GB Generator ==="
    echo "Started: $(date)"
    
    sudo -u postgres dropdb --if-exists "$DB_NAME"
    sudo -u postgres createdb "$DB_NAME"
    sudo -u postgres psql -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
    
    sudo -u postgres psql -d "$DB_NAME" << 'EOSQL'
CREATE OR REPLACE FUNCTION large_random_bytes(size_bytes INT) RETURNS BYTEA AS $$
DECLARE r BYTEA := E'\x'; c INT := 1024; m INT := size_bytes;
BEGIN
    WHILE m > 0 LOOP
        IF m >= c THEN r := r || gen_random_bytes(c); m := m - c;
        ELSE r := r || gen_random_bytes(m); m := 0; END IF;
    END LOOP;
    RETURN r;
END; $$ LANGUAGE plpgsql;

CREATE TABLE enterprise_documents (
    id BIGSERIAL PRIMARY KEY, uuid UUID DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT now(), document_type VARCHAR(50),
    document_name VARCHAR(255), file_size BIGINT, content BYTEA
);
ALTER TABLE enterprise_documents ALTER COLUMN content SET STORAGE EXTERNAL;
CREATE INDEX idx_doc_created ON enterprise_documents(created_at);

CREATE TABLE enterprise_transactions (
    id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT now(),
    customer_id BIGINT, amount DECIMAL(15,2), status VARCHAR(20)
);
EOSQL

    echo "Tables created"
    batch=0
    start=$(date +%s)
    
    while true; do
        sz=$(sudo -u postgres psql -t -A -c "SELECT pg_database_size('$DB_NAME')/1024/1024/1024")
        [ "$sz" -ge "$TARGET_GB" ] && echo "=== Target reached: ${sz}GB ===" && break
        
        batch=$((batch + 1))
        pct=$((sz * 100 / TARGET_GB))
        el=$(($(date +%s) - start))
        if [ $sz -gt 0 ] && [ $el -gt 0 ]; then
            eta="$(((TARGET_GB-sz)*el/sz/60))min"
        else
            eta="..."
        fi
        
        echo "Batch $batch: ${sz}GB/${TARGET_GB}GB (${pct}%) ETA:$eta"
        
        sudo -u postgres psql -q -d "$DB_NAME" -c "
INSERT INTO enterprise_documents (document_type, document_name, file_size, content)
SELECT (ARRAY['PDF','DOCX','IMG','VID'])[floor(random()*4+1)],
       'Doc_'||i||'_'||substr(md5(random()::TEXT),1,8), $BLOB_BYTES,
       large_random_bytes($BLOB_BYTES)
FROM generate_series(1, $BATCH_ROWS) i;"

        sudo -u postgres psql -q -d "$DB_NAME" -c "
INSERT INTO enterprise_transactions (customer_id, amount, status)
SELECT (random()*1000000)::BIGINT, (random()*10000)::DECIMAL(15,2),
       (ARRAY['ok','pending','failed'])[floor(random()*3+1)]
FROM generate_series(1, 20000);"
    done
    
    sudo -u postgres psql -d "$DB_NAME" -c "ANALYZE;"
    sudo -u postgres psql -d "$DB_NAME" -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME')) as size, (SELECT count(*) FROM enterprise_documents) as docs;"
    echo "Completed: $(date)"
}

case "${1:-help}" in
    status) cmd_status ;;
    generate) cmd_generate ;;
    generate-bg) cmd_generate_bg ;;
    stop) cmd_stop ;;
    drop) cmd_drop ;;
    drop-all) cmd_drop_all ;;
    backup) cmd_backup ;;
    estimate) cmd_estimate ;;
    log) cmd_log ;;
    attach) cmd_attach ;;
    _run) do_generate ;;
    help|--help|-h) show_help ;;
    *) echo "Unknown: $1"; show_help ;;
esac
