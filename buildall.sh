#!/bin/bash
# buildall.sh — Cross-compile dbbackup for all platforms into bins/
# Optionally deploy the matching binary on the current node.
set -e

# ── Colors ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ── Prerequisites ──
if ! command -v go &>/dev/null; then
    echo -e "${RED}Error: Go is not installed or not in PATH${NC}"
    exit 1
fi

# ── Build metadata ──
APP_NAME="dbbackup"
VERSION=$(grep 'version.*=' main.go | head -1 | sed 's/.*"\(.*\)".*/\1/')
BUILD_TIME=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS="-w -s -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME} -X main.gitCommit=${GIT_COMMIT}"
OUT_DIR="bins"

# ── Target platforms ──
# Format: GOOS/GOARCH[:suffix[:GOAMD64]]
TARGETS=(
    "linux/amd64"
    "linux/amd64:_v3:v3"
    "linux/arm64"
    "linux/arm:_armv7"
    "darwin/amd64"
    "darwin/arm64"
)

echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${BLUE}  buildall.sh — Cross-compile ${APP_NAME} v${VERSION}${NC}"
echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════${NC}"
echo -e "  Commit:  ${YELLOW}${GIT_COMMIT}${NC}"
echo -e "  Time:    ${YELLOW}${BUILD_TIME}${NC}"
echo -e "  Output:  ${YELLOW}${OUT_DIR}/${NC}"
echo ""

# ── Prepare output directory ──
rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"

# ── Build all targets ──
total=${#TARGETS[@]}
ok=0
fail=0

for i in "${!TARGETS[@]}"; do
    entry="${TARGETS[$i]}"
    n=$((i + 1))

    # Parse GOOS/GOARCH[:suffix[:GOAMD64]]
    IFS=':' read -r platform suffix goamd64 <<< "$entry"
    GOOS="${platform%%/*}"
    GOARCH="${platform##*/}"

    bin_name="${APP_NAME}_${GOOS}_${GOARCH}${suffix}"
    label="${platform}${suffix}"
    [[ -n "$goamd64" ]] && label+="  (GOAMD64=${goamd64})"
    echo -ne "  [${n}/${total}] ${BOLD}${label}${NC} ... "

    # Set GOAMD64 for AVX2-optimized builds
    build_env=(CGO_ENABLED=0 GOOS="$GOOS" GOARCH="$GOARCH")
    [[ -n "$goamd64" ]] && build_env+=(GOAMD64="$goamd64")

    if env "${build_env[@]}" \
        go build -trimpath -ldflags "$LDFLAGS" -o "${OUT_DIR}/${bin_name}" . 2>/dev/null; then
        # File size
        sz=$(stat -c%s "${OUT_DIR}/${bin_name}" 2>/dev/null || stat -f%z "${OUT_DIR}/${bin_name}" 2>/dev/null || echo 0)
        mb=$(awk "BEGIN{printf \"%.1f\", ${sz}/1048576}")
        echo -e "${GREEN}OK${NC}  ${bin_name}  (${mb} MB)"
        ok=$((ok + 1))
    else
        echo -e "${RED}FAIL${NC}"
        fail=$((fail + 1))
    fi
done

echo ""
echo -e "${BOLD}${GREEN}Build complete:${NC} ${ok}/${total} succeeded"
if [ $fail -gt 0 ]; then
    echo -e "${YELLOW}  ${fail} target(s) failed${NC}"
fi
echo ""

# ── List built binaries ──
echo -e "${CYAN}Binaries in ${OUT_DIR}/:${NC}"
ls -lh "${OUT_DIR}"/ | tail -n +2 | awk '{printf "  %-40s %s\n", $9, $5}'
echo ""

# ── Detect current platform ──
HOST_OS=$(uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH=$(uname -m)
case "$HOST_ARCH" in
    x86_64)  HOST_ARCH="amd64" ;;
    aarch64) HOST_ARCH="arm64" ;;
    armv7l)  HOST_ARCH="arm_armv7" ;;
esac

LOCAL_BIN="${OUT_DIR}/${APP_NAME}_${HOST_OS}_${HOST_ARCH}"
# Try with armv7 suffix if standard name not found
if [ ! -f "$LOCAL_BIN" ] && [ "$HOST_ARCH" = "arm" ]; then
    LOCAL_BIN="${OUT_DIR}/${APP_NAME}_${HOST_OS}_arm_armv7"
fi

if [ ! -f "$LOCAL_BIN" ]; then
    echo -e "${YELLOW}No matching binary for this node (${HOST_OS}/${HOST_ARCH}).${NC}"
    exit 0
fi

# ── Ask about local deployment ──
DEPLOY_TARGET="/usr/local/bin/${APP_NAME}"

echo -e "${BOLD}Deploy on this node?${NC}"
echo -e "  Binary:  ${CYAN}${LOCAL_BIN}${NC}"
echo -e "  Target:  ${CYAN}${DEPLOY_TARGET}${NC}"
echo ""
read -r -p "Install to ${DEPLOY_TARGET}? [y/N] " answer

case "$answer" in
    [yY]|[yY][eE][sS])
        # Check if we need sudo
        if [ -w "$(dirname "$DEPLOY_TARGET")" ]; then
            cp "$LOCAL_BIN" "$DEPLOY_TARGET"
            chmod +x "$DEPLOY_TARGET"
        else
            echo -e "  ${YELLOW}Requires sudo${NC}"
            sudo cp "$LOCAL_BIN" "$DEPLOY_TARGET"
            sudo chmod +x "$DEPLOY_TARGET"
        fi

        # Verify
        if [ -x "$DEPLOY_TARGET" ]; then
            installed_ver=$("$DEPLOY_TARGET" version --format short 2>/dev/null || "$DEPLOY_TARGET" --version 2>/dev/null || echo "unknown")
            echo -e "  ${GREEN}Deployed ${APP_NAME} v${VERSION} → ${DEPLOY_TARGET}${NC}"
            echo -e "  ${GREEN}Installed version: ${installed_ver}${NC}"
        else
            echo -e "  ${RED}Deployment failed${NC}"
            exit 1
        fi
        ;;
    *)
        echo -e "  Skipped deployment."
        ;;
esac
