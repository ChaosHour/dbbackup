#!/bin/bash
# Release script for dbbackup
# Builds binaries and creates/updates GitHub release
#
# Usage:
#   ./release.sh              # Build and release current version
#   ./release.sh --bump       # Bump patch version, build, and release
#   ./release.sh --update     # Update existing release with new binaries
#   ./release.sh --fast       # Fast release (skip tests, parallel builds)
#   ./release.sh --dry-run    # Show what would happen without doing it

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Configuration
TOKEN_FILE=".gh_token"
MAIN_FILE="main.go"

# Security: List of files that should NEVER be committed (regex patterns)
SECURITY_FILES=(
    "\.gh_token"
    "\.env$"
    "\.env\.local"
    "\.env\.production"
    "\.pem$"
    "\.key$"
    "\.p12$"
    "\.dbbackup\.conf"
    "secrets\.yaml"
    "secrets\.json"
    "\.aws/credentials"
    "\.gcloud/.*\.json"
)

# Parse arguments
BUMP_VERSION=false
UPDATE_ONLY=false
DRY_RUN=false
FAST_MODE=false
RELEASE_MSG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --bump)
            BUMP_VERSION=true
            shift
            ;;
        --update)
            UPDATE_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --fast)
            FAST_MODE=true
            shift
            ;;
        -m|--message)
            RELEASE_MSG="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --bump           Bump patch version before release"
            echo "  --update         Update existing release (don't create new)"
            echo "  --fast           Fast mode: parallel builds, skip tests"
            echo "  --dry-run        Show what would happen without doing it"
            echo "  -m, --message    Release message/comment (required for new releases)"
            echo "  --help           Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 -m \"Fix TUI crash on cluster restore\""
            echo "  $0 --bump -m \"Add new backup compression option\""
            echo "  $0 --fast -m \"Hotfix release\""
            echo "  $0 --update  # Just update binaries, no message needed"
            echo ""
            echo "Security:"
            echo "  Token file: .gh_token (gitignored)"
            echo "  Never commits: .env, *.pem, *.key, secrets.*, .dbbackup.conf"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage"
            exit 1
            ;;
    esac
done

# Check for GitHub token
if [ ! -f "$TOKEN_FILE" ]; then
    echo -e "${RED}❌ Token file not found: $TOKEN_FILE${NC}"
    echo ""
    echo "Create it with:"
    echo "  echo 'your_github_token' > $TOKEN_FILE"
    echo ""
    echo "The file is gitignored for security."
    exit 1
fi

GH_TOKEN=$(cat "$TOKEN_FILE" | tr -d '[:space:]')
if [ -z "$GH_TOKEN" ]; then
    echo -e "${RED}❌ Token file is empty${NC}"
    exit 1
fi

export GH_TOKEN

# Security check: Ensure sensitive files are not staged
echo -e "${BLUE}🔒 Security check...${NC}"
check_security() {
    local found_issues=false
    
    # Check if any security files are staged
    for pattern in "${SECURITY_FILES[@]}"; do
        staged=$(git diff --cached --name-only 2>/dev/null | grep -E "$pattern" || true)
        if [ -n "$staged" ]; then
            echo -e "${RED}❌ SECURITY: Sensitive file staged for commit: $staged${NC}"
            found_issues=true
        fi
    done
    
    # Check for hardcoded tokens/secrets in staged files
    if git diff --cached 2>/dev/null | grep -iE "(api_key|apikey|secret|token|password|passwd).*=.*['\"][^'\"]{8,}['\"]" | head -3; then
        echo -e "${YELLOW}⚠️  WARNING: Possible secrets detected in staged changes${NC}"
        echo "   Review carefully before committing!"
    fi
    
    if [ "$found_issues" = true ]; then
        echo -e "${RED}❌ Aborting release due to security issues${NC}"
        echo "   Remove sensitive files: git reset HEAD <file>"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Security check passed${NC}"
    export SECURITY_VALIDATED=true
}

# Run security check unless dry-run
if [ "$DRY_RUN" = false ]; then
    check_security
fi

# Check CHANGELOG.md mentions current version
echo -e "${BLUE}📋 Checking CHANGELOG...${NC}"
CURRENT_VERSION_CHECK=$(grep 'version.*=' "$MAIN_FILE" | head -1 | sed 's/.*"\(.*\)".*/\1/')
if ! grep -q "$CURRENT_VERSION_CHECK" CHANGELOG.md 2>/dev/null; then
    echo -e "${YELLOW}⚠️  WARNING: CHANGELOG.md does not mention version ${CURRENT_VERSION_CHECK}${NC}"
    echo -e "   Consider updating CHANGELOG.md before releasing."
    read -p "   Continue anyway? [y/N] " -r
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}Aborting.${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✅ CHANGELOG.md mentions v${CURRENT_VERSION_CHECK}${NC}"
fi

# Run pre-release suite (unless --fast)
if [ "$FAST_MODE" = false ]; then
    if [ -f "scripts/pre_release_suite.sh" ]; then
        echo ""
        echo -e "${BLUE}🧪 Running pre-release validation suite...${NC}"
        if bash scripts/pre_release_suite.sh --quick; then
            echo -e "${GREEN}✅ Pre-release suite passed${NC}"
        else
            echo -e "${RED}❌ Pre-release suite failed${NC}"
            read -p "   Continue anyway? [y/N] " -r
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                echo -e "${RED}Aborting.${NC}"
                exit 1
            fi
        fi
    fi
else
    echo -e "${YELLOW}⚡ Skipping pre-release suite (--fast)${NC}"
fi

# Get current version
CURRENT_VERSION=$(grep 'version.*=' "$MAIN_FILE" | head -1 | sed 's/.*"\(.*\)".*/\1/')
echo -e "${BLUE}📦 Current version: ${YELLOW}${CURRENT_VERSION}${NC}"

# Bump version if requested
if [ "$BUMP_VERSION" = true ]; then
    # Parse version (X.Y.Z)
    MAJOR=$(echo "$CURRENT_VERSION" | cut -d. -f1)
    MINOR=$(echo "$CURRENT_VERSION" | cut -d. -f2)
    PATCH=$(echo "$CURRENT_VERSION" | cut -d. -f3)
    
    NEW_PATCH=$((PATCH + 1))
    NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"
    
    echo -e "${GREEN}📈 Bumping version: ${YELLOW}${CURRENT_VERSION}${NC} → ${GREEN}${NEW_VERSION}${NC}"
    
    if [ "$DRY_RUN" = false ]; then
        sed -i "s/\(version[[:space:]]*=[[:space:]]*\)\"${CURRENT_VERSION}\"/\1\"${NEW_VERSION}\"/" "$MAIN_FILE"
        CURRENT_VERSION="$NEW_VERSION"
    fi
fi

TAG="v${CURRENT_VERSION}"
echo -e "${BLUE}🏷️  Release tag: ${YELLOW}${TAG}${NC}"

# Require message for new releases (not updates)
if [ -z "$RELEASE_MSG" ] && [ "$UPDATE_ONLY" = false ] && [ "$DRY_RUN" = false ]; then
    echo -e "${RED}❌ Release message required. Use -m \"Your message\"${NC}"
    echo ""
    echo "Example:"
    echo "  $0 -m \"Fix TUI crash on cluster restore\""
    exit 1
fi

if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}🔍 DRY RUN - No changes will be made${NC}"
    echo ""
    echo "Would execute:"
    echo "  1. Security check (verify no tokens/secrets staged)"
    echo "  2. Build binaries with build_all.sh"
    if [ "$FAST_MODE" = true ]; then
        echo "     (FAST MODE: parallel builds, skip tests)"
    fi
    echo "  3. Commit and push changes"
    echo "  4. Create/update release ${TAG}"
    exit 0
fi

# Build binaries
echo ""
echo -e "${BOLD}${BLUE}🔨 Building binaries...${NC}"

if [ "$FAST_MODE" = true ]; then
    echo -e "${YELLOW}⚡ Fast mode: parallel builds, skipping tests${NC}"
    
    # Fast parallel build
    START_TIME=$(date +%s)
    
    # Build all platforms in parallel
    PLATFORMS=(
        "linux/amd64"
        "linux/arm64"
        "linux/arm/7"
        "darwin/amd64"
        "darwin/arm64"
    )
    
    mkdir -p bin
    
    # Get version info for ldflags
    VERSION=$(grep 'version.*=' "$MAIN_FILE" | head -1 | sed 's/.*"\(.*\)".*/\1/')
    BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
    LDFLAGS="-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME} -X main.gitCommit=${GIT_COMMIT}"
    
    # Build in parallel using background jobs
    pids=()
    for platform in "${PLATFORMS[@]}"; do
        GOOS=$(echo "$platform" | cut -d/ -f1)
        GOARCH=$(echo "$platform" | cut -d/ -f2)
        GOARM=$(echo "$platform" | cut -d/ -f3)
        
        OUTPUT="bin/dbbackup_${GOOS}_${GOARCH}"
        if [ -n "$GOARM" ]; then
            OUTPUT="bin/dbbackup_${GOOS}_arm_armv${GOARM}"
        fi
        
        (
            if [ -n "$GOARM" ]; then
                GOOS=$GOOS GOARCH=arm GOARM=$GOARM go build -trimpath -ldflags "$LDFLAGS" -o "$OUTPUT" . 2>/dev/null
            else
                GOOS=$GOOS GOARCH=$GOARCH go build -trimpath -ldflags "$LDFLAGS" -o "$OUTPUT" . 2>/dev/null
            fi
            if [ $? -eq 0 ]; then
                echo -e "  ${GREEN}✅${NC} $OUTPUT"
            else
                echo -e "  ${RED}❌${NC} $OUTPUT"
            fi
        ) &
        pids+=($!)
    done
    
    # Wait for all builds
    for pid in "${pids[@]}"; do
        wait $pid
    done
    
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo -e "${GREEN}⚡ Fast build completed in ${DURATION}s${NC}"
else
    # Standard build with full checks
    bash build_all.sh
fi

# Check if there are changes to commit
if [ -n "$(git status --porcelain)" ]; then
    echo ""
    echo -e "${BLUE}📝 Committing changes...${NC}"
    git add -A
    
    # Generate commit message using the release message
    if [ -n "$RELEASE_MSG" ]; then
        COMMIT_MSG="${TAG}: ${RELEASE_MSG}"
    elif [ "$BUMP_VERSION" = true ]; then
        COMMIT_MSG="${TAG}: Version bump"
    else
        COMMIT_MSG="${TAG}: Release build"
    fi
    
    git commit -m "$COMMIT_MSG"
fi

# Push changes to all remotes
REMOTES=("origin")
if git remote | grep -q '^uuxo$'; then
    REMOTES+=("uuxo")
fi

for remote in "${REMOTES[@]}"; do
    echo -e "${BLUE}⬆️  Pushing to ${remote}...${NC}"
    git push "$remote" main
done

# Handle tag
TAG_EXISTS=$(git tag -l "$TAG")
if [ -z "$TAG_EXISTS" ]; then
    echo -e "${BLUE}🏷️  Creating tag ${TAG}...${NC}"
    git tag "$TAG"
    for remote in "${REMOTES[@]}"; do
        echo -e "${BLUE}🏷️  Pushing tag to ${remote}...${NC}"
        git push "$remote" "$TAG"
    done
else
    echo -e "${YELLOW}⚠️  Tag ${TAG} already exists${NC}"
fi

# Check if release exists
echo ""
echo -e "${BLUE}🚀 Preparing release...${NC}"

RELEASE_EXISTS=$(gh release view "$TAG" 2>/dev/null && echo "yes" || echo "no")

if [ "$RELEASE_EXISTS" = "yes" ] || [ "$UPDATE_ONLY" = true ]; then
    echo -e "${YELLOW}📦 Updating existing release ${TAG}...${NC}"
    
    # Delete existing assets and upload new ones
    for binary in bin/dbbackup_*; do
        if [ -f "$binary" ]; then
            ASSET_NAME=$(basename "$binary")
            echo "  Uploading $ASSET_NAME..."
            gh release upload "$TAG" "$binary" --clobber
        fi
    done
else
    echo -e "${GREEN}📦 Creating new release ${TAG}...${NC}"
    
    # Generate release notes with the provided message
    NOTES="## ${TAG}: ${RELEASE_MSG}

### Downloads
| Platform | Architecture | Binary |
|----------|--------------|--------|
| Linux | x86_64 (Intel/AMD) | \`dbbackup_linux_amd64\` |
| Linux | ARM64 | \`dbbackup_linux_arm64\` |
| Linux | ARMv7 | \`dbbackup_linux_arm_armv7\` |
| macOS | Intel | \`dbbackup_darwin_amd64\` |
| macOS | Apple Silicon (M1/M2) | \`dbbackup_darwin_arm64\` |

### Installation
\`\`\`bash
# Linux x86_64
curl -LO https://github.com/PlusOne/dbbackup/releases/download/${TAG}/dbbackup_linux_amd64
chmod +x dbbackup_linux_amd64
sudo mv dbbackup_linux_amd64 /usr/local/bin/dbbackup

# macOS Apple Silicon
curl -LO https://github.com/PlusOne/dbbackup/releases/download/${TAG}/dbbackup_darwin_arm64
chmod +x dbbackup_darwin_arm64
sudo mv dbbackup_darwin_arm64 /usr/local/bin/dbbackup
\`\`\`
"

    gh release create "$TAG" \
        --title "${TAG}: ${RELEASE_MSG}" \
        --notes "$NOTES" \
        bin/dbbackup_linux_amd64 \
        bin/dbbackup_linux_arm64 \
        bin/dbbackup_linux_arm_armv7 \
        bin/dbbackup_darwin_amd64 \
        bin/dbbackup_darwin_arm64
fi

echo ""
echo -e "${GREEN}${BOLD}✅ Release complete!${NC}"
echo -e "   ${BLUE}https://github.com/PlusOne/dbbackup/releases/tag/${TAG}${NC}"

# Summary
echo ""
echo -e "${BOLD}📊 Release Summary:${NC}"
echo -e "   Version: ${TAG}"
echo -e "   Remotes: ${REMOTES[*]}"
echo -e "   Mode: $([ "$FAST_MODE" = true ] && echo "Fast (parallel)" || echo "Standard")"
echo -e "   Security: $([ -n "$SECURITY_VALIDATED" ] && echo "${GREEN}Validated${NC}" || echo "Checked")"
if [ "$FAST_MODE" = true ] && [ -n "$DURATION" ]; then
    echo -e "   Build time: ${DURATION}s"
fi
