#!/bin/bash
# Galera Cluster integration test runner
#
# Usage:
#   ./tests/galera/run_tests.sh          # Start cluster + run tests + stop
#   ./tests/galera/run_tests.sh --keep   # Keep cluster running after tests
#   ./tests/galera/run_tests.sh --down   # Just stop the cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Parse args
KEEP_RUNNING=false
DOWN_ONLY=false
for arg in "$@"; do
    case $arg in
        --keep) KEEP_RUNNING=true ;;
        --down) DOWN_ONLY=true ;;
    esac
done

# Stop cluster
if [ "$DOWN_ONLY" = true ]; then
    echo -e "${BLUE}Stopping Galera cluster...${NC}"
    docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    echo -e "${GREEN}Done.${NC}"
    exit 0
fi

echo -e "${BLUE}=== Galera Cluster Integration Tests ===${NC}"
echo ""

# Start cluster
echo -e "${BLUE}[1/4] Starting 3-node Galera cluster...${NC}"
docker-compose -f "$COMPOSE_FILE" up -d 2>&1

# Wait for health
echo -e "${BLUE}[2/4] Waiting for cluster health...${NC}"
MAX_WAIT=120
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    HEALTHY=$(docker-compose -f "$COMPOSE_FILE" ps --format json 2>/dev/null | grep -c '"healthy"' || echo "0")
    if [ "$HEALTHY" -ge 3 ]; then
        echo -e "${GREEN}  All 3 nodes healthy after ${WAITED}s${NC}"
        break
    fi
    sleep 5
    WAITED=$((WAITED + 5))
    echo -e "  Waiting... ${WAITED}s (${HEALTHY}/3 healthy)"
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo -e "${RED}  Cluster did not become healthy within ${MAX_WAIT}s${NC}"
    docker-compose -f "$COMPOSE_FILE" logs --tail=20
    docker-compose -f "$COMPOSE_FILE" down -v
    exit 1
fi

# Quick cluster check
echo -e "${BLUE}[3/4] Verifying cluster state...${NC}"
CLUSTER_SIZE=$(docker exec galera-1 mariadb -uroot -ptestpass -N -e "SHOW STATUS LIKE 'wsrep_cluster_size'" 2>/dev/null | awk '{print $2}')
echo -e "  Cluster size: ${CLUSTER_SIZE}"
if [ "$CLUSTER_SIZE" != "3" ]; then
    echo -e "${YELLOW}  WARNING: Expected cluster size 3, got ${CLUSTER_SIZE}${NC}"
fi

# Run integration tests
echo -e "${BLUE}[4/4] Running Go integration tests...${NC}"
echo ""
cd "$PROJECT_DIR"

if go test -v ./internal/engine/ -run TestGaleraIntegration -tags=integration -timeout 60s; then
    echo ""
    echo -e "${GREEN}=== All Galera integration tests PASSED ===${NC}"
    RESULT=0
else
    echo ""
    echo -e "${RED}=== Some Galera integration tests FAILED ===${NC}"
    RESULT=1
fi

# Cleanup unless --keep
if [ "$KEEP_RUNNING" = false ]; then
    echo ""
    echo -e "${BLUE}Stopping Galera cluster...${NC}"
    docker-compose -f "$COMPOSE_FILE" down -v 2>/dev/null
    echo -e "${GREEN}Cluster stopped.${NC}"
else
    echo ""
    echo -e "${YELLOW}Cluster still running (--keep). Stop with:${NC}"
    echo "  docker-compose -f $COMPOSE_FILE down -v"
fi

exit $RESULT
