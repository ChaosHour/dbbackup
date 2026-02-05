#!/bin/bash
#
# fakedbcreator.sh - Create PostgreSQL test database of specified size
#
# Usage: ./fakedbcreator.sh <size_in_gb> [database_name]
# Examples:
#   ./fakedbcreator.sh 100          # Create 100GB 'fakedb' database
#   ./fakedbcreator.sh 200 testdb   # Create 200GB 'testdb' database
#
set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[✗]${NC} $1"; }

show_usage() {
    echo "Usage: $0 <size_in_gb> [database_name]"
    echo ""
    echo "Arguments:"
    echo "  size_in_gb    Target size in gigabytes (1-500)"
    echo "  database_name Database name (default: fakedb)"
    echo ""
    echo "Examples:"
    echo "  $0 100           # Create 100GB 'fakedb' database"
    echo "  $0 200 testdb    # Create 200GB 'testdb' database"
    echo "  $0 50 benchmark  # Create 50GB 'benchmark' database"
    echo ""
    echo "Features:"
    echo "  - Creates wide tables (100+ columns)"
    echo "  - JSONB documents with nested structures"
    echo "  - Large TEXT and BYTEA fields"
    echo "  - Multiple schemas (core, logs, documents, analytics)"
    echo "  - Realistic enterprise data patterns"
    exit 1
}

if [ "$#" -lt 1 ]; then
    show_usage
fi

SIZE_GB="$1"
DB_NAME="${2:-fakedb}"

# Validate inputs
if ! [[ "$SIZE_GB" =~ ^[0-9]+$ ]] || [ "$SIZE_GB" -lt 1 ] || [ "$SIZE_GB" -gt 500 ]; then
    log_error "Size must be between 1 and 500 GB"
    exit 1
fi

# Check for required tools
command -v bc >/dev/null 2>&1 || { log_error "bc is required: apt install bc"; exit 1; }
command -v psql >/dev/null 2>&1 || { log_error "psql is required"; exit 1; }

# Check if running as postgres or can sudo
if [ "$(whoami)" = "postgres" ]; then
    PSQL_CMD="psql"
    CREATEDB_CMD="createdb"
else
    PSQL_CMD="sudo -u postgres psql"
    CREATEDB_CMD="sudo -u postgres createdb"
fi

# Estimate time
MINUTES_PER_10GB=5
ESTIMATED_MINUTES=$(echo "$SIZE_GB * $MINUTES_PER_10GB / 10" | bc)

echo ""
echo "============================================================================="
echo -e "${GREEN}PostgreSQL Fake Database Creator${NC}"
echo "============================================================================="
echo ""
log_info "Target size:     ${SIZE_GB} GB"
log_info "Database name:   ${DB_NAME}"
log_info "Estimated time:  ~${ESTIMATED_MINUTES} minutes"
echo ""

# Check if database exists
if $PSQL_CMD -lqt 2>/dev/null | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    log_warn "Database '$DB_NAME' already exists!"
    read -p "Drop and recreate? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Dropping existing database..."
        $PSQL_CMD -c "DROP DATABASE IF EXISTS \"$DB_NAME\";" 2>/dev/null || true
    else
        log_error "Aborted."
        exit 1
    fi
fi

# Create database
log_info "Creating database '$DB_NAME'..."
$CREATEDB_CMD "$DB_NAME" 2>/dev/null || {
    log_error "Failed to create database. Check PostgreSQL is running."
    exit 1
}
log_success "Database created"

# Generate and execute SQL directly (no temp file for large sizes)
log_info "Generating schema and data..."

# Create schema and helper functions
$PSQL_CMD -d "$DB_NAME" -q << 'SCHEMA_SQL'
-- Schemas
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS logs;
CREATE SCHEMA IF NOT EXISTS documents;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Random text generator
CREATE OR REPLACE FUNCTION core.random_text(min_words integer, max_words integer)
RETURNS text AS $$
DECLARE
    words text[] := ARRAY[
        'lorem', 'ipsum', 'dolor', 'sit', 'amet', 'consectetur', 'adipiscing', 'elit',
        'sed', 'do', 'eiusmod', 'tempor', 'incididunt', 'ut', 'labore', 'et', 'dolore',
        'magna', 'aliqua', 'enterprise', 'database', 'performance', 'scalability'
    ];
    word_count integer := min_words + (random() * (max_words - min_words))::integer;
    result text := '';
BEGIN
    FOR i IN 1..word_count LOOP
        result := result || words[1 + (random() * (array_length(words, 1) - 1))::integer] || ' ';
    END LOOP;
    RETURN trim(result);
END;
$$ LANGUAGE plpgsql;

-- Random JSONB generator
CREATE OR REPLACE FUNCTION core.random_json_document()
RETURNS jsonb AS $$
BEGIN
    RETURN jsonb_build_object(
        'version', (random() * 10)::integer,
        'priority', CASE (random() * 3)::integer WHEN 0 THEN 'low' WHEN 1 THEN 'medium' ELSE 'high' END,
        'metadata', jsonb_build_object(
            'created_by', 'user_' || (random() * 10000)::integer,
            'department', CASE (random() * 5)::integer
                WHEN 0 THEN 'engineering' WHEN 1 THEN 'sales' WHEN 2 THEN 'marketing' ELSE 'support' END,
            'active', random() > 0.5
        ),
        'content_hash', md5(random()::text)
    );
END;
$$ LANGUAGE plpgsql;

-- Binary data generator (larger sizes for realistic BLOBs)
CREATE OR REPLACE FUNCTION core.random_binary(size_kb integer)
RETURNS bytea AS $$
DECLARE
    result bytea := '';
    chunks_needed integer := LEAST((size_kb * 1024) / 16, 100000);  -- Cap at ~1.6MB per call
BEGIN
    FOR i IN 1..chunks_needed LOOP
        result := result || decode(md5(random()::text || i::text), 'hex');
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Large object creator (PostgreSQL LO - true BLOBs)
CREATE OR REPLACE FUNCTION core.create_large_object(size_mb integer)
RETURNS oid AS $$
DECLARE
    lo_oid oid;
    fd integer;
    chunk bytea;
    chunks_needed integer := size_mb * 64;  -- 64 x 16KB chunks = 1MB
BEGIN
    lo_oid := lo_create(0);
    fd := lo_open(lo_oid, 131072);  -- INV_WRITE
    FOR i IN 1..chunks_needed LOOP
        chunk := decode(repeat(md5(random()::text), 1024), 'hex');  -- 16KB chunk
        PERFORM lowrite(fd, chunk);
    END LOOP;
    PERFORM lo_close(fd);
    RETURN lo_oid;
END;
$$ LANGUAGE plpgsql;

-- Main documents table (stores most of the data)
CREATE TABLE documents.enterprise_documents (
    id bigserial PRIMARY KEY,
    uuid uuid DEFAULT gen_random_uuid(),
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    title varchar(500),
    content text,
    metadata jsonb,
    binary_data bytea,
    status varchar(50) DEFAULT 'active',
    version integer DEFAULT 1,
    owner_id integer,
    department varchar(100),
    tags text[],
    search_vector tsvector
);

-- Audit log
CREATE TABLE logs.audit_log (
    id bigserial PRIMARY KEY,
    timestamp timestamptz DEFAULT now(),
    user_id integer,
    action varchar(100),
    resource_id bigint,
    old_value jsonb,
    new_value jsonb,
    ip_address inet
);

-- Analytics
CREATE TABLE analytics.events (
    id bigserial PRIMARY KEY,
    event_time timestamptz DEFAULT now(),
    event_type varchar(100),
    user_id integer,
    properties jsonb,
    duration_ms integer
);

-- ============================================
-- EXOTIC PostgreSQL data types table
-- ============================================
CREATE TABLE core.exotic_types (
    id bigserial PRIMARY KEY,
    
    -- Network types
    ip_addr inet,
    mac_addr macaddr,
    cidr_block cidr,
    
    -- Geometric types  
    geo_point point,
    geo_line line,
    geo_box box,
    geo_circle circle,
    geo_polygon polygon,
    geo_path path,
    
    -- Range types
    int_range int4range,
    num_range numrange,
    date_range daterange,
    ts_range tstzrange,
    
    -- Other special types
    bit_field bit(64),
    varbit_field bit varying(256),
    money_amount money,
    xml_data xml,
    tsvec tsvector,
    tsquery_data tsquery,
    
    -- Arrays
    int_array integer[],
    text_array text[],
    float_array float8[],
    json_array jsonb[],
    
    -- Composite and misc
    interval_data interval,
    uuid_field uuid DEFAULT gen_random_uuid()
);

-- ============================================
-- Large Objects tracking table
-- ============================================
CREATE TABLE documents.large_objects (
    id bigserial PRIMARY KEY,
    name varchar(255),
    mime_type varchar(100),
    lo_oid oid,           -- PostgreSQL large object OID
    size_bytes bigint,
    created_at timestamptz DEFAULT now(),
    checksum text
);

-- ============================================
-- Partitioned table (time-based)
-- ============================================
CREATE TABLE logs.time_series_data (
    id bigserial,
    ts timestamptz NOT NULL DEFAULT now(),
    metric_name varchar(100),
    metric_value double precision,
    labels jsonb,
    PRIMARY KEY (ts, id)
) PARTITION BY RANGE (ts);

-- Create partitions
CREATE TABLE logs.time_series_data_2024 PARTITION OF logs.time_series_data
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE logs.time_series_data_2025 PARTITION OF logs.time_series_data
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- ============================================
-- Materialized view
-- ============================================
CREATE MATERIALIZED VIEW analytics.event_summary AS
SELECT 
    event_type,
    date_trunc('hour', event_time) as hour,
    count(*) as event_count,
    avg(duration_ms) as avg_duration
FROM analytics.events
GROUP BY event_type, date_trunc('hour', event_time);

-- Indexes
CREATE INDEX idx_docs_uuid ON documents.enterprise_documents(uuid);
CREATE INDEX idx_docs_created ON documents.enterprise_documents(created_at);
CREATE INDEX idx_docs_metadata ON documents.enterprise_documents USING gin(metadata);
CREATE INDEX idx_docs_search ON documents.enterprise_documents USING gin(search_vector);
CREATE INDEX idx_audit_timestamp ON logs.audit_log(timestamp);
CREATE INDEX idx_events_time ON analytics.events(event_time);
CREATE INDEX idx_exotic_ip ON core.exotic_types USING gist(ip_addr inet_ops);
CREATE INDEX idx_exotic_geo ON core.exotic_types USING gist(geo_point);
CREATE INDEX idx_time_series ON logs.time_series_data(metric_name, ts);
SCHEMA_SQL

log_success "Schema created"

# Calculate batch parameters
# Target: ~20KB per row in enterprise_documents = ~50K rows per GB
ROWS_PER_GB=50000
TOTAL_ROWS=$((SIZE_GB * ROWS_PER_GB))
BATCH_SIZE=10000
BATCHES=$((TOTAL_ROWS / BATCH_SIZE))

log_info "Inserting $TOTAL_ROWS rows in $BATCHES batches..."

# Start time tracking
START_TIME=$(date +%s)

for batch in $(seq 1 $BATCHES); do
    # Progress display
    PROGRESS=$((batch * 100 / BATCHES))
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $batch -gt 1 ] && [ $ELAPSED -gt 0 ]; then
        ROWS_DONE=$((batch * BATCH_SIZE))
        RATE=$((ROWS_DONE / ELAPSED))
        REMAINING_ROWS=$((TOTAL_ROWS - ROWS_DONE))
        if [ $RATE -gt 0 ]; then
            ETA_SECONDS=$((REMAINING_ROWS / RATE))
            ETA_MINUTES=$((ETA_SECONDS / 60))
            echo -ne "\r${CYAN}[PROGRESS]${NC} Batch $batch/$BATCHES (${PROGRESS}%) | ${ROWS_DONE} rows | ${RATE} rows/s | ETA: ${ETA_MINUTES}m   "
        fi
    else
        echo -ne "\r${CYAN}[PROGRESS]${NC} Batch $batch/$BATCHES (${PROGRESS}%)   "
    fi

    # Insert batch
    $PSQL_CMD -d "$DB_NAME" -q << BATCH_SQL
INSERT INTO documents.enterprise_documents (title, content, metadata, binary_data, department, tags)
SELECT 
    'Document-' || g || '-' || md5(random()::text),
    core.random_text(100, 500),
    core.random_json_document(),
    core.random_binary(16),
    CASE (random() * 5)::integer
        WHEN 0 THEN 'engineering' WHEN 1 THEN 'sales' WHEN 2 THEN 'marketing' 
        WHEN 3 THEN 'support' ELSE 'operations' END,
    ARRAY['tag_' || (random()*100)::int, 'tag_' || (random()*100)::int]
FROM generate_series(1, $BATCH_SIZE) g;

INSERT INTO logs.audit_log (user_id, action, resource_id, old_value, new_value, ip_address)
SELECT 
    (random() * 10000)::integer,
    CASE (random() * 4)::integer WHEN 0 THEN 'create' WHEN 1 THEN 'update' WHEN 2 THEN 'delete' ELSE 'view' END,
    (random() * 1000000)::bigint,
    core.random_json_document(),
    core.random_json_document(),
    ('192.168.' || (random() * 255)::integer || '.' || (random() * 255)::integer)::inet
FROM generate_series(1, $((BATCH_SIZE / 2))) g;

INSERT INTO analytics.events (event_type, user_id, properties, duration_ms)
SELECT 
    CASE (random() * 5)::integer WHEN 0 THEN 'page_view' WHEN 1 THEN 'click' WHEN 2 THEN 'purchase' ELSE 'custom' END,
    (random() * 100000)::integer,
    core.random_json_document(),
    (random() * 60000)::integer
FROM generate_series(1, $((BATCH_SIZE * 2))) g;

-- Exotic types (smaller batch for variety)
INSERT INTO core.exotic_types (
    ip_addr, mac_addr, cidr_block,
    geo_point, geo_line, geo_box, geo_circle, geo_polygon, geo_path,
    int_range, num_range, date_range, ts_range,
    bit_field, varbit_field, money_amount, xml_data, tsvec, tsquery_data,
    int_array, text_array, float_array, json_array, interval_data
)
SELECT
    ('10.' || (random()*255)::int || '.' || (random()*255)::int || '.' || (random()*255)::int)::inet,
    ('08:00:2b:' || lpad(to_hex((random()*255)::int), 2, '0') || ':' || lpad(to_hex((random()*255)::int), 2, '0') || ':' || lpad(to_hex((random()*255)::int), 2, '0'))::macaddr,
    ('10.' || (random()*255)::int || '.0.0/16')::cidr,
    point(random()*360-180, random()*180-90),
    line(point(random()*100, random()*100), point(random()*100, random()*100)),
    box(point(random()*50, random()*50), point(50+random()*50, 50+random()*50)),
    circle(point(random()*100, random()*100), random()*50),
    polygon(box(point(random()*50, random()*50), point(50+random()*50, 50+random()*50))),
    ('((' || random()*100 || ',' || random()*100 || '),(' || random()*100 || ',' || random()*100 || '),(' || random()*100 || ',' || random()*100 || '))')::path,
    int4range((random()*100)::int, (100+random()*100)::int),
    numrange((random()*100)::numeric, (100+random()*100)::numeric),
    daterange(current_date - (random()*365)::int, current_date + (random()*365)::int),
    tstzrange(now() - (random()*1000 || ' hours')::interval, now() + (random()*1000 || ' hours')::interval),
    (floor(random()*9223372036854775807)::bigint)::bit(64),
    (floor(random()*65535)::int)::bit(16)::bit varying(256),
    (random()*10000)::numeric::money,
    ('<data><id>' || g || '</id><value>' || random() || '</value></data>')::xml,
    to_tsvector('english', 'sample searchable text with random ' || md5(random()::text)),
    to_tsquery('english', 'search & text'),
    ARRAY[(random()*1000)::int, (random()*1000)::int, (random()*1000)::int],
    ARRAY['tag_' || (random()*100)::int, 'item_' || (random()*100)::int, md5(random()::text)],
    ARRAY[random(), random(), random(), random(), random()],
    ARRAY[core.random_json_document(), core.random_json_document()],
    ((random()*1000)::int || ' hours ' || (random()*60)::int || ' minutes')::interval
FROM generate_series(1, $((BATCH_SIZE / 10))) g;

-- Time series data (for partitioned table)
INSERT INTO logs.time_series_data (ts, metric_name, metric_value, labels)
SELECT
    timestamp '2024-01-01' + (random() * 730 || ' days')::interval + (random() * 86400 || ' seconds')::interval,
    CASE (random() * 5)::integer 
        WHEN 0 THEN 'cpu_usage' WHEN 1 THEN 'memory_used' WHEN 2 THEN 'disk_io'
        WHEN 3 THEN 'network_rx' ELSE 'requests_per_sec' END,
    random() * 100,
    jsonb_build_object('host', 'server-' || (random()*50)::int, 'dc', 'dc-' || (random()*3)::int)
FROM generate_series(1, $((BATCH_SIZE / 5))) g;
BATCH_SQL

done

echo ""  # New line after progress
log_success "Data insertion complete"

# Create large objects (true PostgreSQL BLOBs)
log_info "Creating large objects (true BLOBs)..."
NUM_LARGE_OBJECTS=$((SIZE_GB * 2))  # 2 large objects per GB (1-5MB each)
$PSQL_CMD -d "$DB_NAME" << LARGE_OBJ_SQL
DO \$\$
DECLARE
    lo_oid oid;
    size_mb int;
    i int;
BEGIN
    FOR i IN 1..$NUM_LARGE_OBJECTS LOOP
        size_mb := 1 + (random() * 4)::int;  -- 1-5 MB each
        lo_oid := core.create_large_object(size_mb);
        INSERT INTO documents.large_objects (name, mime_type, lo_oid, size_bytes, checksum)
        VALUES (
            'blob_' || i || '_' || md5(random()::text) || '.bin',
            CASE (random() * 4)::int 
                WHEN 0 THEN 'application/pdf' 
                WHEN 1 THEN 'image/png' 
                WHEN 2 THEN 'application/zip'
                ELSE 'application/octet-stream' END,
            lo_oid,
            size_mb * 1024 * 1024,
            md5(random()::text)
        );
        IF i % 10 = 0 THEN
            RAISE NOTICE 'Created large object % of $NUM_LARGE_OBJECTS', i;
        END IF;
    END LOOP;
END;
\$\$;
LARGE_OBJ_SQL
log_success "Large objects created ($NUM_LARGE_OBJECTS BLOBs)"

# Update search vectors
log_info "Updating search vectors..."
$PSQL_CMD -d "$DB_NAME" -q << 'FINALIZE_SQL'
UPDATE documents.enterprise_documents 
SET search_vector = to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content, ''));
ANALYZE;
FINALIZE_SQL
log_success "Search vectors updated"

# Get final stats
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
DURATION_MINUTES=$((DURATION / 60))

DB_SIZE=$($PSQL_CMD -d "$DB_NAME" -t -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));" | tr -d ' ')
ROW_COUNT=$($PSQL_CMD -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM documents.enterprise_documents;" | tr -d ' ')
LO_COUNT=$($PSQL_CMD -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM documents.large_objects;" | tr -d ' ')
LO_SIZE=$($PSQL_CMD -d "$DB_NAME" -t -c "SELECT pg_size_pretty(COALESCE(SUM(size_bytes), 0)::bigint) FROM documents.large_objects;" | tr -d ' ')

echo ""
echo "============================================================================="
echo -e "${GREEN}Database Creation Complete${NC}"
echo "============================================================================="
echo ""
echo "  Database:       $DB_NAME"
echo "  Target Size:    ${SIZE_GB} GB"
echo "  Actual Size:    $DB_SIZE"
echo "  Documents:      $ROW_COUNT rows"
echo "  Large Objects:  $LO_COUNT BLOBs ($LO_SIZE)"
echo "  Duration:       ${DURATION_MINUTES} minutes (${DURATION}s)"
echo ""
echo "Data Types Included:"
echo "  - Standard: TEXT, JSONB, BYTEA, TIMESTAMPTZ, INET, UUID"
echo "  - Arrays:   INTEGER[], TEXT[], FLOAT8[], JSONB[]"
echo "  - Geometric: POINT, LINE, BOX, CIRCLE, POLYGON, PATH"
echo "  - Ranges:   INT4RANGE, NUMRANGE, DATERANGE, TSTZRANGE"
echo "  - Special:  XML, TSVECTOR, TSQUERY, MONEY, BIT, MACADDR, CIDR"
echo "  - BLOBs:    Large Objects (pg_largeobject)"
echo "  - Partitioned tables, Materialized views"
echo ""
echo "Tables:"
$PSQL_CMD -d "$DB_NAME" -c "
SELECT 
    schemaname || '.' || tablename as table_name,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as size
FROM pg_tables 
WHERE schemaname IN ('core', 'logs', 'documents', 'analytics')
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;
"
echo ""
echo "Test backup command:"
echo "  dbbackup backup --database $DB_NAME"
echo ""
echo "============================================================================="
